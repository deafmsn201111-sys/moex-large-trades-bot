"""
Клиент для T-Bank Invest API.

Использует WebSocket-стрим для получения сделок в реальном времени
(задержка ~1 секунда вместо 15 минут у MOEX ISS).
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterator, Dict, List, Optional

from tinkoff.invest import (
    Client,
    InstrumentKind,
    TradeDirection,
)
from tinkoff.invest.aio import (
    AioInstrumentsService,
    AioMarketDataStreamService,
)

logger = logging.getLogger("tinkoff-client")


@dataclass
class Trade:
    """Универсальная структура сделки."""
    ticker: str
    figi: str
    trade_id: Optional[str]
    trade_time: Optional[str]
    price: Optional[float]
    quantity: Optional[float]
    value: float
    buy_sell: Optional[str]  # "B" | "S" | None

    @property
    def id_num(self) -> int:
        if self.trade_id is None:
            return 0
        try:
            return int(str(self.trade_id))
        except Exception:
            return 0

    @property
    def sort_key(self):
        return (self.trade_time or "", self.id_num)

    @property
    def is_buy(self) -> Optional[bool]:
        if self.buy_sell is None:
            return None
        return str(self.buy_sell).upper().startswith("B")

    @property
    def direction_emoji(self) -> str:
        if self.buy_sell is None:
            return "⚪"
        if str(self.buy_sell).upper().startswith("B"):
            return "🟢"
        return "🔴"

    @property
    def direction_text(self) -> str:
        if self.buy_sell is None:
            return "?"
        if str(self.buy_sell).upper().startswith("B"):
            return "ПОКУПКА"
        return "ПРОДАЖА"

    @property
    def dedup_key(self) -> str:
        if self.trade_id:
            return f"tinkoff:{self.figi}:{self.trade_id}"
        # Fallback если нет trade_id
        return "|".join([
            "tinkoff",
            self.figi,
            str(self.trade_time or ""),
            str(self.price if self.price is not None else ""),
            str(self.quantity if self.quantity is not None else ""),
            str(self.value),
            str(self.buy_sell or ""),
        ])


def quotation_to_float(q) -> Optional[float]:
    """Конвертирует Quotation (units + nano) в float."""
    if q is None:
        return None
    try:
        return float(q.units) + float(q.nano) / 1e9
    except Exception:
        return None


def timestamp_to_str(ts) -> Optional[str]:
    """Конвертирует protobuf Timestamp в строку HH:MM:SS."""
    if ts is None:
        return None
    try:
        dt = datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9)
        return dt.strftime("%H:%M:%S")
    except Exception:
        return None


def direction_to_str(direction) -> Optional[str]:
    """Конвертирует TradeDirection в 'B'/'S'/None."""
    if direction == TradeDirection.TRADE_DIRECTION_BUY:
        return "B"
    if direction == TradeDirection.TRADE_DIRECTION_SELL:
        return "S"
    return None


class TinkoffClient:
    """
    Асинхронный клиент T-Bank Invest API.

    Основные возможности:
    - resolve_figi: тикер → FIGI
    - stream_trades: WebSocket-стрим сделок в реальном времени
    - get_last_trades: последние сделки (fallback после reconnect)
    """

    def __init__(self, token: str):
        self._token = token
        # Кеш тикер → FIGI
        self._figi_cache: Dict[str, str] = {}
        # Кеш FIGI → тикер (для обратного маппинга)
        self._ticker_cache: Dict[str, str] = {}

    async def resolve_figi(self, ticker: str) -> Optional[str]:
        """
        Резолвит тикер Мосбиржи (например, SBER) в FIGI.
        Использует FindInstrument API.
        """
        ticker = ticker.upper().strip()

        if ticker in self._figi_cache:
            return self._figi_cache[ticker]

        try:
            async with AioInstrumentsService(self._token) as service:
                response = await service.find_instrument(
                    query=ticker,
                    instrument_kind=InstrumentKind.INSTRUMENT_KIND_SHARE,
                )

                for inst in response.instruments:
                    if inst.ticker.upper() == ticker:
                        figi = inst.figi
                        self._figi_cache[ticker] = figi
                        self._ticker_cache[figi] = ticker
                        logger.info(
                            "Resolved %s -> FIGI=%s (%s)",
                            ticker, figi, inst.name,
                        )
                        return figi

            logger.warning("Ticker %s not found in T-Bank instruments", ticker)
            return None

        except Exception as e:
            logger.error("Failed to resolve FIGI for %s: %s", ticker, e)
            return None

    def get_ticker_by_figi(self, figi: str) -> Optional[str]:
        """Обратный маппинг FIGI → тикер."""
        return self._ticker_cache.get(figi)

    def _convert_trade(self, raw_trade, figi: str, ticker: str) -> Trade:
        """Конвертирует protobuf Trade в нашу структуру."""
        price = quotation_to_float(raw_trade.price)
        quantity = float(raw_trade.quantity) if raw_trade.quantity else None

        # Сумма сделки = цена * количество
        if price is not None and quantity is not None:
            value = abs(price * quantity)
        else:
            value = 0.0

        return Trade(
            ticker=ticker,
            figi=figi,
            trade_id=str(raw_trade.trade_id) if raw_trade.trade_id else None,
            trade_time=timestamp_to_str(raw_trade.time),
            price=price,
            quantity=quantity,
            value=value,
            buy_sell=direction_to_str(raw_trade.direction),
        )

    async def get_last_trades(
        self,
        figi: str,
        limit: int = 100,
    ) -> List[Trade]:
        """
        Возвращает последние сделки по FIGI.
        Используется как fallback после reconnect.
        """
        from tinkoff.invest.aio import AioMarketDataService

        ticker = self.get_ticker_by_figi(figi) or figi

        try:
            async with AioMarketDataService(self._token) as service:
                response = await service.get_last_trades(
                    figi=figi,
                )

                trades = []
                for raw_trade in response.trades:
                    trades.append(
                        self._convert_trade(raw_trade, figi, ticker)
                    )

                # Берём только последние N
                return trades[-limit:] if len(trades) > limit else trades

        except Exception as e:
            logger.error("get_last_trades failed for %s: %s", figi, e)
            return []

    async def stream_trades(
        self,
        figis: List[str],
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
    ) -> AsyncIterator[Trade]:
        """
        WebSocket-стрим сделок в реальном времени.

        Автоматически переподключается при обрывах с экспоненциальным backoff.
        """
        current_delay = reconnect_delay

        while True:
            try:
                logger.info(
                    "Starting trades stream for %d FIGIs...", len(figis)
                )

                async with AioMarketDataStreamService(self._token) as stream:
                    # Подписка на стрим сделок
                    logger.info("Subscribing to trades stream...")

                    async for market_data in stream.trades_stream(figis):
                        # Сбрасываем задержку при успешном получении данных
                        current_delay = reconnect_delay

                        # Проверяем, что в сообщении есть сделка
                        if not market_data.HasField("trade"):
                            continue

                        raw_trade = market_data.trade
                        figi = raw_trade.figi
                        ticker = self.get_ticker_by_figi(figi) or figi

                        trade = self._convert_trade(raw_trade, figi, ticker)
                        yield trade

            except asyncio.CancelledError:
                logger.info("Trades stream cancelled")
                raise
            except Exception as e:
                logger.error(
                    "Trades stream error: %s. Reconnecting in %.1fs...",
                    e, current_delay,
                )
                await asyncio.sleep(current_delay)
                # Экспоненциальный backoff с потолком
                current_delay = min(current_delay * 2, max_reconnect_delay)
