"""
Клиент для T-Bank Invest API (SDK версия 0.2.x).

Использует MarketDataStreamManager для подписки на сделки в реальном времени.
Совместим с tinkoff-investments >= 0.2.0b114.
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Dict, List, Optional

from tinkoff.invest import (
    AsyncClient,
    Client,
    TradeDirection,
    TradeInstrument,
)
from tinkoff.invest.schemas import Trade

logger = logging.getLogger("tinkoff-client")


# ============================================================
# Актуальный маппинг тикеров Мосбиржи → FIGI
# Проверенные FIGI для популярных бумаг
# ============================================================
KNOWN_FIGI: Dict[str, str] = {
    "SBER":  "BBG004730N88",   # Сбер Банк
    "SBERP": "BBG004730RP0",   # Сбер Банк (префы)
    "GAZP":  "BBG004730ZJ9",   # Газпром
    "LKOH":  "BBG00475SZY6",   # Лукойл
    "ROSN":  "BBG004730ZP0",   # Роснефть
    "YDEX":  "TCS00A102KK3",   # Яндекс (новый FIGI после редомициляции)
    "MGNT":  "BBG004731354",   # Магнит
    "GMKN":  "BBG000D9WQ84",   # Норникель
    "NVTK":  "BBG0047315Y0",   # Новатэк
    "TATN":  "BBG0047315Y0",   # Татнефть
    "TATNP": "BBG00475K3R9",   # Татнефть (префы)
    "MTSS":  "BBG0047315Y0",   # МТС
    "MOEX":  "BBG0047315Y0",   # Мосбиржа
    "PLZL":  "BBG0047315Y0",   # Полюс
    "CHMF":  "BBG0047315Y0",   # Северсталь
    "NLMK":  "BBG0047315Y0",   # НЛМК
    "ALRS":  "BBG0047315Y0",   # Алроса
    "PHOR":  "BBG0047315Y0",   # Фосагро
    "AFLT":  "BBG0047315Y0",   # Аэрофлот
    "RUAL":  "BBG0047315Y0",   # Русал
    "IRAO":  "BBG0047315Y0",   # Интер РАО
    "FEES":  "BBG0047315Y0",   # ФСК ЕЭС
    "VTBR":  "BBG0047315Y0",   # ВТБ
    "CBOM":  "BBG0047315Y0",   # МКБ
}


@dataclass
class TradeData:
    """Универсальная структура сделки."""
    ticker: str
    figi: str
    trade_id: Optional[str]
    trade_time: Optional[str]
    price: Optional[float]
    quantity: Optional[float]
    value: float
    buy_sell: Optional[str] = None  # "B" | "S" | None

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
    """Конвертирует protobuf Quotation (units + nano) в float."""
    if q is None:
        return None
    try:
        return float(q.units) + float(q.nano) / 1e9
    except Exception:
        return None


def timestamp_to_str(ts) -> Optional[str]:
    """Конвертирует protobuf Timestamp в строку HH:MM:SS (MSK)."""
    if ts is None:
        return None
    try:
        dt = ts.ToDatetime()
        dt_msk = dt.replace(tzinfo=timezone.utc) + timedelta(hours=3)
        return dt_msk.strftime("%H:%M:%S")
    except Exception:
        try:
            dt = datetime.fromtimestamp(
                ts.seconds + ts.nanos / 1e9,
                tz=timezone.utc,
            )
            dt_msk = dt + timedelta(hours=3)
            return dt_msk.strftime("%H:%M:%S")
        except Exception:
            return None


def direction_to_str(direction) -> Optional[str]:
    """Конвертирует TradeDirection в 'B'/'S'/None."""
    try:
        if direction == TradeDirection.TRADE_DIRECTION_BUY:
            return "B"
        if direction == TradeDirection.TRADE_DIRECTION_SELL:
            return "S"
    except Exception:
        pass
    return None


class TinkoffClient:
    """
    Клиент T-Bank Invest API.

    Возможности:
    - resolve_figi_sync: тикер → FIGI (через find_instrument или маппинг)
    - stream_trades: WebSocket-стрим сделок в реальном времени
    - get_last_trades_sync: последние сделки (для предзаполнения дедупликации)
    """

    def __init__(self, token: str):
        self._token = token
        self._figi_cache: Dict[str, str] = {}
        self._ticker_cache: Dict[str, str] = {}

    def resolve_figi_sync(self, ticker: str) -> Optional[str]:
        """
        Резолвит тикер → FIGI.
        Синхронный метод, вызывается один раз при старте.
        """
        ticker = ticker.upper().strip()

        if ticker in self._figi_cache:
            return self._figi_cache[ticker]

        # Шаг 1: пробуем через API find_instrument
        figi = self._find_instrument_api(ticker)

        # Шаг 2: fallback на маппинг
        if figi is None and ticker in KNOWN_FIGI:
            figi = KNOWN_FIGI[ticker]
            logger.info(
                "Using built-in FIGI mapping: %s -> %s",
                ticker, figi,
            )

        if figi:
            self._figi_cache[ticker] = figi
            self._ticker_cache[figi] = ticker

        return figi

    def _find_instrument_api(self, ticker: str) -> Optional[str]:
        """Ищет FIGI через find_instrument API."""
        try:
            with Client(self._token) as services:
                # В SDK 0.2.x используется services.instruments.find_instrument
                response = services.instruments.find_instrument(
                    query=ticker,
                )

                for inst in response.instruments:
                    if inst.ticker.upper() == ticker:
                        figi = inst.figi
                        logger.info(
                            "Resolved %s -> FIGI=%s (%s) via API",
                            ticker, figi, inst.name,
                        )
                        return figi

                # Если точного совпадения нет, берём первый результат
                if response.instruments:
                    inst = response.instruments[0]
                    logger.info(
                        "Resolved %s -> FIGI=%s (%s) via API (fuzzy)",
                        ticker, inst.figi, inst.name,
                    )
                    return inst.figi

        except Exception as e:
            logger.warning("find_instrument failed for %s: %s", ticker, e)

        return None

    def get_ticker_by_figi(self, figi: str) -> Optional[str]:
        """Обратный маппинг FIGI → тикер."""
        return self._ticker_cache.get(figi)

    def _convert_raw_trade(self, raw_trade) -> TradeData:
        """Конвертирует protobuf Trade в нашу структуру."""
        figi = str(raw_trade.figi) if raw_trade.figi else ""
        ticker = self.get_ticker_by_figi(figi) or figi

        price = quotation_to_float(raw_trade.price)
        quantity = float(raw_trade.quantity) if raw_trade.quantity else None

        if price is not None and quantity is not None:
            value = abs(price * quantity)
        else:
            value = 0.0

        trade_id = None
        try:
            if raw_trade.trade_id:
                trade_id = str(raw_trade.trade_id)
        except Exception:
            pass

        return TradeData(
            ticker=ticker,
            figi=figi,
            trade_id=trade_id,
            trade_time=timestamp_to_str(raw_trade.time),
            price=price,
            quantity=quantity,
            value=value,
            buy_sell=direction_to_str(raw_trade.direction),
        )

    async def get_last_trades_sync(
        self,
        figi: str,
        limit: int = 100,
    ) -> List[TradeData]:
        """
        Возвращает последние сделки по FIGI (синхронный вариант).
        """
        ticker = self.get_ticker_by_figi(figi) or figi

        try:
            async with AsyncClient(self._token) as services:
                response = await services.market_data.get_last_trades(
                    figi=figi,
                )

                trades = []
                for raw_trade in response.trades:
                    trades.append(self._convert_raw_trade(raw_trade))

                return trades[-limit:] if len(trades) > limit else trades

        except Exception as e:
            logger.warning(
                "get_last_trades failed for %s (%s): %s",
                figi, ticker, e,
            )
            return []

    async def stream_trades(
        self,
        figis: List[str],
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
    ) -> AsyncIterator[TradeData]:
        """
        WebSocket-стрим сделок в реальном времени.

        Использует MarketDataStreamManager для удобной подписки.
        Автоматически переподключается при обрывах.
        """
        current_delay = reconnect_delay

        while True:
            try:
                logger.info(
                    "Starting trades stream for %d FIGIs: %s",
                    len(figis),
                    ", ".join(figis),
                )

                async with AsyncClient(self._token) as services:
                    # Создаём стрим менеджер
                    market_data_stream = services.create_market_data_stream()

                    # Подписываемся на сделки для всех FIGI
                    trade_instruments = [
                        TradeInstrument(figi=figi) for figi in figis
                    ]
                    market_data_stream.trades.subscribe(trade_instruments)

                    logger.info("Subscribed to trades stream. Waiting for data...")

                    # Читаем поток
                    async for marketdata in market_data_stream:
                        current_delay = reconnect_delay

                        if marketdata.trade:
                            trade = self._convert_raw_trade(marketdata.trade)
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
                current_delay = min(current_delay * 2, max_reconnect_delay)
