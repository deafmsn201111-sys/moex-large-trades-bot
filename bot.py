import asyncio
import logging
import os
import time
from collections import deque
from typing import Dict, List, Optional

import httpx
from aiohttp import web
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from dotenv import load_dotenv

from config import AppConfig, TickerConfig, load_config
from tinkoff_client import TinkoffClient, TradeData

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

logger = logging.getLogger("moex-bot")
logger.setLevel(logging.INFO)


def _env_int(name: str, default: int, min_value: int, max_value: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except Exception:
        value = default
    return max(min_value, min(value, max_value))


class BoundedSet:
    def __init__(self, max_size: int):
        self.max_size = max(1, int(max_size))
        self._items = set()
        self._order = deque()

    def add(self, value: str) -> bool:
        if value in self._items:
            return False
        self._items.add(value)
        self._order.append(value)
        while len(self._order) > self.max_size:
            old = self._order.popleft()
            self._items.discard(old)
        return True

    def __len__(self):
        return len(self._items)


def fmt_money(value: Optional[float]) -> str:
    if value is None:
        return "?"
    try:
        return f"{int(round(float(value))):,}".replace(",", " ")
    except Exception:
        return str(value)


def fmt_price(value: Optional[float]) -> str:
    if value is None:
        return "?"
    try:
        return f"{float(value):,.2f}".replace(",", " ")
    except Exception:
        return str(value)


def fmt_qty(value: Optional[float]) -> str:
    if value is None:
        return "?"
    try:
        value = float(value)
        if abs(value) >= 1:
            return f"{value:,.0f}".replace(",", " ")
        return f"{value:,.4f}".replace(",", " ")
    except Exception:
        return str(value)


def format_trade_text(trade: TradeData) -> str:
    direction_emoji = trade.direction_emoji
    direction_text = trade.direction_text

    if direction_text == "ПОКУПКА":
        header = f"{direction_emoji}📈 ПОКУПКА (BUY)"
    elif direction_text == "ПРОДАЖА":
        header = f"{direction_emoji}📉 ПРОДАЖА (SELL)"
    else:
        header = f"{direction_emoji} Сделка"

    lines = [
        header,
        f"<b>{trade.ticker}</b>",
        "",
        f"💰 Сумма: <b>{fmt_money(trade.value)} ₽</b>",
        f"💵 Цена: {fmt_price(trade.price)}",
        f"📦 Кол-во: {fmt_qty(trade.quantity)}",
        f"🕐 Время: {trade.trade_time or '?'}",
        f"🆔 № сделки: {trade.trade_id or '?'}",
        "",
        f'<a href="https://www.moex.com/ru/issue.aspx?code={trade.ticker}">Открыть на MOEX</a>',
    ]
    return "\n".join(lines)


async def health(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def start_health_server() -> Optional[web.AppRunner]:
    port_raw = os.getenv("PORT")
    if not port_raw:
        logger.info("PORT is not set; health server disabled")
        return None
    try:
        port = int(port_raw)
    except Exception:
        logger.exception("Invalid PORT value: %s", port_raw)
        raise

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health server started on port %s", port)
    return runner


async def self_ping() -> None:
    interval = _env_int("SELF_PING_INTERVAL_SECONDS", 300, 30, 3600)
    base_url = os.getenv("SELF_PING_URL", "").strip()
    if not base_url:
        base_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if not base_url:
        logger.info("SELF_PING_URL and RENDER_EXTERNAL_URL are not set; self-ping disabled")
        return
    if not base_url.startswith("http://") and not base_url.startswith("https://"):
        base_url = f"https://{base_url}"

    url = base_url.rstrip("/") + "/health"
    logger.info("Self-ping enabled: %s every %s seconds", url, interval)

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                response = await client.get(url)
                logger.debug("Self-ping status: %s", response.status_code)
            except Exception as exc:
                logger.warning("Self-ping failed: %s", exc)
            await asyncio.sleep(interval)


async def send_alerts_batch(bot: Bot, chat_id: str, trades: List[TradeData]) -> None:
    """Отправляет группу трейдов одним сообщением."""
    MAX_TRADES_PER_MSG = 10

    if not trades:
        return

    trades_sorted = sorted(trades, key=lambda t: t.sort_key, reverse=True)

    for i in range(0, len(trades_sorted), MAX_TRADES_PER_MSG):
        batch = trades_sorted[i:i + MAX_TRADES_PER_MSG]

        if len(batch) == 1:
            text = format_trade_text(batch[0])
        else:
            parts = []
            for trade in batch:
                direction = trade.direction_emoji
                parts.append(
                    f"{direction} <b>{trade.ticker}</b> | "
                    f"{fmt_money(trade.value)} ₽ | "
                    f"{fmt_price(trade.price)} | "
                    f"{fmt_qty(trade.quantity)} шт"
                )
            text = f"🔥 <b>{len(batch)} крупных сделок:</b>\n\n" + "\n".join(parts)

        for attempt in range(5):
            try:
                await bot.send_message(
                    chat_id,
                    text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                logger.info("Sent batch to Telegram: %d trades", len(batch))
                break
            except TelegramRetryAfter as exc:
                logger.warning("Telegram flood limit, retry after %s", exc.retry_after)
                await asyncio.sleep(exc.retry_after)
            except TelegramAPIError as exc:
                logger.error("Telegram API error: %s", exc)
                break
            except Exception:
                logger.exception("Unexpected error while sending")
                break


async def alert_sender(
    name: str,
    bot: Bot,
    chat_id: str,
    queue: asyncio.Queue,
    batch_interval: float = 0.3,
) -> None:
    """Собирает трейды в батчи и отправляет группами."""
    while True:
        batch: List[TradeData] = []

        try:
            while True:
                trade: TradeData = queue.get_nowait()
                batch.append(trade)
                queue.task_done()
                if len(batch) >= 50:
                    break
        except asyncio.QueueEmpty:
            pass

        if not batch:
            try:
                trade: TradeData = await queue.get()
                batch.append(trade)
                queue.task_done()
            except Exception:
                continue

        await asyncio.sleep(batch_interval)

        try:
            while True:
                trade: TradeData = queue.get_nowait()
                batch.append(trade)
                queue.task_done()
                if len(batch) >= 50:
                    break
        except asyncio.QueueEmpty:
            pass

        await send_alerts_batch(bot, chat_id, batch)


async def process_trade(
    trade: TradeData,
    cfg: AppConfig,
    tickers_by_figi: Dict[str, TickerConfig],
    seen: BoundedSet,
    alert_queue: asyncio.Queue,
) -> bool:
    """
    Обрабатывает одну сделку из стрима.
    Возвращает True если сделка прошла фильтр и попала в очередь алертов.
    """
    # Дедупликация
    if not seen.add(trade.dedup_key):
        return False

    # Находим порог для этого тикера
    ticker_cfg = tickers_by_figi.get(trade.figi)
    if ticker_cfg is None:
        return False

    threshold = ticker_cfg.min_value_rub
    if threshold is None:
        threshold = cfg.default_min_value_rub

    # Фильтр по сумме
    if trade.value < threshold:
        return False

    logger.info(
        "ALERT: %s %s trade value=%.0f RUB, price=%s, qty=%s, id=%s",
        trade.direction_text,
        trade.ticker,
        trade.value,
        trade.price,
        trade.quantity,
        trade.trade_id,
    )

    try:
        alert_queue.put_nowait(trade)
    except asyncio.QueueFull:
        logger.warning(
            "Alert queue full, dropping trade %s %s",
            trade.ticker,
            trade.trade_id,
        )
        return False

    return True


async def main() -> None:
    cfg = load_config()

    health_runner = await start_health_server()
    self_ping_task = asyncio.create_task(self_ping())

    bot = Bot(token=cfg.bot_token)
    tinkoff = TinkoffClient(cfg.tinvest_token)

    workers = []

    try:
        me = await bot.get_me()
        logger.info("Bot authorized as @%s", me.username)
    except Exception:
        logger.exception("Cannot authorize bot. Check BOT_TOKEN.")
        if self_ping_task:
            self_ping_task.cancel()
        if health_runner:
            await health_runner.cleanup()
        await bot.session.close()
        raise

    try:
        try:
            chat = await bot.get_chat(cfg.chat_id)
            logger.info(
                "Chat accessible: id=%s title=%s",
                getattr(chat, "id", cfg.chat_id),
                getattr(chat, "title", "?"),
            )
        except Exception as exc:
            logger.error("Cannot validate chat %s: %s", cfg.chat_id, exc)

        if cfg.send_start_message:
            try:
                await bot.send_message(cfg.chat_id, "✅ Бот запущен.")
            except Exception:
                logger.exception("Cannot send start message.")

        # ============================================================
        # Шаг 1: Резолвим FIGI для всех тикеров
        # ============================================================
        logger.info("Resolving FIGI for %d tickers...", len(cfg.tickers))

        resolved_tickers: List[TickerConfig] = []
        failed_tickers: List[str] = []

        for ticker_cfg in cfg.tickers:
            if ticker_cfg.figi:
                # FIGI задан в конфиге — используем его
                resolved = ticker_cfg
            else:
                # Резолвим через API
                figi = await asyncio.to_thread(tinkoff.resolve_figi_sync, ticker_cfg.ticker)
                if figi:
                    resolved = TickerConfig(
                        ticker=ticker_cfg.ticker,
                        min_value_rub=ticker_cfg.min_value_rub,
                        figi=figi,
                    )
                else:
                    failed_tickers.append(ticker_cfg.ticker)
                    continue

            resolved_tickers.append(resolved)

        if failed_tickers:
            logger.warning(
                "Failed to resolve FIGI for tickers: %s",
                ", ".join(failed_tickers),
            )

        if not resolved_tickers:
            logger.error("No tickers could be resolved. Exiting.")
            return

        # Маппинг FIGI → TickerConfig для быстрого поиска
        tickers_by_figi: Dict[str, TickerConfig] = {
            t.figi: t for t in resolved_tickers
        }

        # Список FIGI для подписки
        figis = [t.figi for t in resolved_tickers]

        logger.info(
            "Resolved %d tickers: %s",
            len(resolved_tickers),
            ", ".join(f"{t.ticker}({t.figi})" for t in resolved_tickers),
        )

                # ============================================================
        # Шаг 2: Предзаполнение дедупликации (последние сделки)
        # ============================================================
        seen = BoundedSet(cfg.max_seen_per_ticker)

        logger.info("Pre-filling seen trades from history...")
        for ticker_cfg in resolved_tickers:
            # get_last_trades теперь реально синхронный — запускаем в потоке
            last_trades = await asyncio.to_thread(
                tinkoff.get_last_trades,
                ticker_cfg.figi,
                100,
            )
            for trade in last_trades:
                seen.add(trade.dedup_key)
            logger.info(
                "%s: pre-filled %d trades",
                ticker_cfg.ticker,
                len(last_trades),
            )

        # ============================================================
        # Шаг 3: Запуск очереди алертов
        # ============================================================
        alert_queue: asyncio.Queue = asyncio.Queue(
            maxsize=_env_int("ALERT_QUEUE_SIZE", 10000, 100, 100000)
        )
        sender_count = _env_int("SEND_WORKERS", 3, 1, 5)

        workers = [
            asyncio.create_task(
                alert_sender(
                    f"sender-{i}",
                    bot,
                    cfg.chat_id,
                    alert_queue,
                    batch_interval=0.3,
                )
            )
            for i in range(sender_count)
        ]

        # ============================================================
        # Шаг 4: Запуск WebSocket-стрима
        # ============================================================
        logger.info("Starting trades stream...")

        last_heartbeat = time.time()
        HEARTBEAT_INTERVAL = 60
        trades_received = 0
        alerts_sent = 0

        async def stream_listener() -> None:
            nonlocal trades_received, alerts_sent

            async for trade in tinkoff.stream_trades(figis):
                trades_received += 1

                if await process_trade(
                    trade=trade,
                    cfg=cfg,
                    tickers_by_figi=tickers_by_figi,
                    seen=seen,
                    alert_queue=alert_queue,
                ):
                    alerts_sent += 1

        stream_task = asyncio.create_task(stream_listener())

        # ============================================================
        # Шаг 5: Heartbeat цикл
        # ============================================================
        logger.info("Bot is running. Waiting for trades...")

        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)

            now = time.time()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                logger.info(
                    "Heartbeat: trades_received=%d, alerts=%d, seen=%d, queue=%d",
                    trades_received,
                    alerts_sent,
                    len(seen),
                    alert_queue.qsize(),
                )
                last_heartbeat = now

    except asyncio.CancelledError:
        raise
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    finally:
        logger.info("Shutting down...")

        if self_ping_task:
            self_ping_task.cancel()

        # Отменяем стрим
        if 'stream_task' in locals() and stream_task:
            stream_task.cancel()
            try:
                await stream_task
            except asyncio.CancelledError:
                pass

        for worker in workers:
            worker.cancel()

        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

        if self_ping_task:
            await asyncio.gather(self_ping_task, return_exceptions=True)

        if health_runner:
            await health_runner.cleanup()

        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
