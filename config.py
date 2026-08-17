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

# Базовая настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Убираем шум от сторонних библиотек
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.WARNING)

# Наш логгер
logger = logging.getLogger("moex-bot")


def _env_int(name: str, default: int, min_value: int, max_value: int) -> int:
    raw = os.getenv(name)

    try:
        value = int(raw) if raw is not None else default
    except Exception:
        value = default

    return max(min_value, min(value, max_value))


class BoundedSet:
    """
    Храним ограниченный набор уже виденных сделок,
    чтобы не отправлять одно и то же повторно.
    """

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
    """
    Запускаем HTTP-сервер, если задан PORT.
    Render Free Web Service требует слушать порт.
    """
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
    """
    Best-effort self-ping для Render Free Web Service.
    """
    interval = _env_int("SELF_PING_INTERVAL_SECONDS", 300, 30, 3600)

    base_url = os.getenv("SELF_PING_URL", "").strip()

    if not base_url:
        base_url = os.getenv("RENDER_EXTERNAL_URL", "").strip()

    if not base_url:
        logger.info(
            "SELF_PING_URL and RENDER_EXTERNAL_URL are not set; self-ping disabled"
        )
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
        except TelegramAPIError:
            logger.exception("Telegram API error while sending trade")
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


async def
