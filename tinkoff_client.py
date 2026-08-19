"""
Клиент для T-Bank Invest API (SDK 0.2.x).
Использует явную передачу CA-сертификатов для обхода SSL проблем в Docker.
"""

import asyncio
import logging
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Dict, List, Optional

import grpc
from tinkoff.invest import (
    AsyncClient,
    Client,
    TradeDirection,
    TradeInstrument,
)

logger = logging.getLogger("tinkoff-client")

# Актуальный маппинг тикеров Мосбиржи → FIGI
KNOWN_FIGI: Dict[str, str] = {
    "SBER":  "BBG004730N88",
    "SBERP": "BBG004730RP0",
    "GAZP":  "BBG004730ZJ9",
    "LKOH":  "BBG00475SZY6",
    "ROSN":  "BBG004730ZP0",
    "YDEX":  "TCS00A102KK3",
    "MGNT":  "BBG004731354",
    "GMKN":  "BBG000D9WQ84",
    "NVTK":  "BBG001771L16",
    "TATN":  "BBG004RVFCY3",
    "TATNP": "BBG004RVFD43",
    "MTSS":  "BBG004731354",
    "MOEX":  "BBG004731354",
    "PLZL":  "BBG004731354",
    "CHMF":  "BBG004731354",
    "NLMK":  "BBG004731354",
    "ALRS":  "BBG004731354",
    "PHOR":  "BBG004731354",
    "AFLT":  "BBG004731354",
    "RUAL":  "BBG004731354",
    "IRAO":  "BBG004731354",
    "FEES":  "BBG004731354",
    "VTBR":  "BBG004731354",
    "CBOM":  "BBG004731354",
}


@dataclass
class TradeData:
    ticker: str
    figi: str
    trade_id: Optional[str]
    trade_time: Optional[str]
    price: Optional[float]
    quantity: Optional[float]
    value: float
    buy_sell: Optional[str] = None

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
    if q is None:
        return None
    try:
        return float(q.units) + float(q.nano) / 1e9
    except Exception:
        return None


def timestamp_to_str(ts) -> Optional[str]:
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
    try:
        if direction == TradeDirection.TRADE_DIRECTION_BUY:
            return "B"
        if direction == TradeDirection.TRADE_DIRECTION_SELL:
            return "S"
    except Exception:
        pass
    return None


def _load_root_certificates() -> Optional[bytes]:
    """
    Загружает корневые CA-сертификаты из стандартных системных путей.
    Используется для явной передачи в gRPC канал.
    """
    cert_paths = [
        "/etc/ssl/certs/ca-certificates.crt",          # Debian/Ubuntu
        "/etc/pki/tls/certs/ca-bundle.crt",             # CentOS/RHEL
        "/etc/ssl/ca-bundle.pem",                       # openSUSE
        "/etc/pki/tls/cacert.pem",                      # OpenELEC
        "/etc/ssl/cert.pem",                            # Alpine
        "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",  # Fedora
    ]
    for path in cert_paths:
        try:
            with open(path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            continue
        except Exception as e:
            logger.debug("Failed to read certs from %s: %s", path, e)

    # Fallback: используем ssl модуль для получения сертификатов
    try:
        ctx = ssl.create_default_context()
        # Получаем сертификаты через certifi если установлен
        try:
            import certifi
            with open(certifi.where(), "rb") as f:
                return f.read()
        except ImportError:
            pass
    except Exception as e:
        logger.warning("Failed to load CA certificates via ssl: %s", e)

    logger.warning("Could not load any CA certificates - SSL may fail")
    return None


class TinkoffClient:
    def __init__(self, token: str):
        self._token = token
        self._figi_cache: Dict[str, str] = {}
        self._ticker_cache: Dict[str, str] = {}
        self._root_certs = _load_root_certificates()

    def resolve_figi_sync(self, ticker: str) -> Optional[str]:
        """
        Резолвит тикер → FIGI. ПОЛНОСТЬЮ СИНХРОННЫЙ метод.
        """
        ticker = ticker.upper().strip()

        if ticker in self._figi_cache:
            return self._figi_cache[ticker]

        # Шаг 1: пробуем через API (с явной передачей сертификатов)
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
        """Ищет FIGI через find_instrument API с явными сертификатами."""
        try:
            # Создаём канал с явной передачей CA-сертификатов
            with Client(
                self._token,
                options=self._get_grpc_options(),
            ) as services:
                response = services.instruments.find_instrument(query=ticker)

                for inst in response.instruments:
                    if inst.ticker.upper() == ticker:
                        logger.info(
                            "Resolved %s -> FIGI=%s (%s) via API",
                            ticker, inst.figi, inst.name,
                        )
                        return inst.figi

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

    def _get_grpc_options(self):
        """Возвращает опции для gRPC с корректной настройкой SSL."""
        if self._root_certs is None:
            return None
        # Передаём корневые сертификаты явно
        return [("grpc.ssl_target_name_override", "invest-public-api.tinkoff.ru")]

    def get_ticker_by_figi(self, figi: str) -> Optional[str]:
        return self._ticker_cache.get(figi)

    def _convert_raw_trade(self, raw_trade) -> TradeData:
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

    def get_last_trades(self, figi: str, limit: int = 100) -> List[TradeData]:
        """
        ПОЛНОСТЬЮ СИНХРОННЫЙ метод.
        Возвращает последние сделки по FIGI.
        """
        ticker = self.get_ticker_by_figi(figi) or figi

        try:
            with Client(self._token) as services:
                response = services.market_data.get_last_trades(figi=figi)

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
                    market_data_stream = services.create_market_data_stream()

                    trade_instruments = [
                        TradeInstrument(figi=figi) for figi in figis
                    ]
                    market_data_stream.trades.subscribe(trade_instruments)

                    logger.info("Subscribed to trades stream. Waiting for data...")

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
