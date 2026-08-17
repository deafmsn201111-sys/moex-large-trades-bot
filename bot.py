import asyncio
import logging
import os
import time
from collections import deque
from typing import Dict, Optional

import httpx
from aiohttp import web
from aiogram import Bot
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from dotenv import load_dotenv

from config import AppConfig, TickerConfig, load_config
from moex_client import MoexClient, Trade

load_dotenv()

# ============================================================
# НАСТРОЙКА ЛОГОВ — ВАЖНО!
# ============================================================
# Сначала настраиваем базовый формат
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Затем глушим шумные библиотеки
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

# Наш логгер
logger = logging.getLogger("moex-bot")
logger.setLevel(logging.INFO)
# ============================================================


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


def source_key(ticker: TickerConfig) -> str:
    return f"{ticker.engine}:{ticker.market}:{ticker.board}:{ticker.ticker}"


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


def format_trade_text(trade: Trade) -> str:
    lines = [
        "🔥 Крупная сделка",
        f"{trade.secid} / {trade.board}",
        f"Сумма: {fmt_money(trade.value)} ₽",
        f"Цена: {fmt_price(trade.price)}",
        f"Кол-во: {fmt_qty(trade.quantity)}",
        f"Время: {trade.trade_time or '?'}",
        f"https://www.moex.com/ru/issue.aspx?board={trade.board}&code={trade.secid}",
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


async def send_trade(bot: Bot, chat_id: str, trade: Trade) -> None:
    text = format_trade_text(trade)

    for attempt in range(5):
        try:
            await bot.send_message(chat_id, text)
            return
        except TelegramRetryAfter as exc:
            logger.warning("Telegram flood limit, retry after %s", exc.retry_after)
            await asyncio.sleep(exc.retry_after)
        except TelegramAPIError as exc:
            logger.error("Telegram API error while sending trade: %s", exc)
            return
        except Exception:
            logger.exception("Unexpected error while sending trade")
            return


async def alert_sender(
    name: str,
    bot: Bot,
    chat_id: str,
    queue: asyncio.Queue,
) -> None:
    while True:
        trade: Trade = await queue.get()

        try:
            await send_trade(bot, chat_id, trade)
        except Exception:
            logger.exception("Alert sender worker failed")
        finally:
            queue.task_done()


async def fetch_trades_safe(
    moex: MoexClient,
    ticker: TickerConfig,
    cfg: AppConfig,
):
    try:
        return await moex.fetch_trades(
            secid=ticker.ticker,
            engine=ticker.engine,
            market=ticker.market,
            board=ticker.board,
            limit=cfg.request_limit,
        )
    except Exception as exc:
        logger.warning("Failed to fetch trades for %s: %s", ticker.ticker, exc)
        return []


async def process_ticker(
    cfg: AppConfig,
    ticker: TickerConfig,
    moex: MoexClient,
    seen: Dict[str, BoundedSet],
    api_semaphore: asyncio.Semaphore,
    alert_queue: asyncio.Queue,
) -> None:
    async with api_semaphore:
        trades = await fetch_trades_safe(moex, ticker, cfg)

    if not trades:
        return

    threshold = ticker.min_value_rub
    if threshold is None:
        threshold = cfg.default_min_value_rub

    bucket = seen[source_key(ticker)]

    for trade in trades:
        if not bucket.add(trade.dedup_key):
            continue

        if trade.value >= threshold:
            try:
                alert_queue.put_nowait(trade)
            except asyncio.QueueFull:
                logger.warning(
                    "Alert queue full, dropping trade %s %s",
                    trade.secid,
                    trade.trade_id,
                )


async def mark_initial_trades(
    cfg: AppConfig,
    moex: MoexClient,
    seen: Dict[str, BoundedSet],
    api_semaphore: asyncio.Semaphore,
) -> None:
    async def mark_one(ticker: TickerConfig) -> None:
        async with api_semaphore:
            trades = await fetch_trades_safe(moex, ticker, cfg)

        bucket = seen[source_key(ticker)]

        for trade in trades:
            bucket.add(trade.dedup_key)

    tasks = [mark_one(ticker) for ticker in cfg.tickers]
    await asyncio.gather(*tasks)


async def main() -> None:
    cfg = load_config()

    health_runner = await start_health_server()
    self_ping_task = asyncio.create_task(self_ping())

    bot = Bot(token=cfg.bot_token)
    moex = MoexClient()

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

        await moex.close()
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
            logger.error(
                "Cannot validate chat %s: %s. "
                "Check TELEGRAM_CHAT_ID (должен начинаться с -100 для каналов!) "
                "и что бот добавлен админом в канал с правом постить.",
                cfg.chat_id,
                exc,
            )

        if cfg.send_start_message:
            try:
                await bot.send_message(
                    cfg.chat_id,
                    "✅ Бот мониторинга крупных сделок запущен.",
                )
            except Exception:
                logger.exception(
                    "Cannot send start message. "
                    "Check TELEGRAM_CHAT_ID and bot admin rights."
                )

        seen: Dict[str, BoundedSet] = {
            source_key(ticker): BoundedSet(cfg.max_seen_per_ticker)
            for ticker in cfg.tickers
        }

        api_semaphore = asyncio.Semaphore(cfg.concurrency)

        alert_queue: asyncio.Queue = asyncio.Queue(
            maxsize=_env_int("ALERT_QUEUE_SIZE", 5000, 100, 100000)
        )

        sender_count = _env_int("SEND_WORKERS", 2, 1, 5)

        workers = [
            asyncio.create_task(
                alert_sender(
                    f"sender-{i}",
                    bot,
                    cfg.chat_id,
                    alert_queue,
                )
            )
            for i in range(sender_count)
        ]

        if not cfg.send_history_on_start:
            await mark_initial_trades(cfg, moex, seen, api_semaphore)

        last_heartbeat = time.time()
        HEARTBEAT_INTERVAL = 60

        logger.info(
            "Starting poll loop: tickers=%s, poll_seconds=%.2f, request_limit=%s",
            len(cfg.tickers),
            cfg.poll_seconds,
            cfg.request_limit,
        )

        while True:
            loop = asyncio.get_running_loop()
            started = loop.time()

            tasks = [
                process_ticker(
                    cfg,
                    ticker,
                    moex,
                    seen,
                    api_semaphore,
                    alert_queue,
                )
                for ticker in cfg.tickers
            ]

            await asyncio.gather(*tasks)

            elapsed = loop.time() - started
            sleep_time = max(0.05, cfg.poll_seconds - elapsed)

            now = time.time()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL:
                logger.info(
                    "Heartbeat: queue_size=%d, seen_trades=%d",
                    alert_queue.qsize(),
                    sum(len(s._items) for s in seen.values()),
                )
                last_heartbeat = now

            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        raise
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt")
    finally:
        if self_ping_task:
            self_ping_task.cancel()

        for worker in workers:
            worker.cancel()

        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

        if self_ping_task:
            await asyncio.gather(self_ping_task, return_exceptions=True)

        if health_runner:
            await health_runner.cleanup()

        await moex.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
