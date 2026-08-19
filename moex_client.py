import hashlib
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("moex-client")

BASE_URL = "https://iss.moex.com/iss"

ID_KEYS = ["TRADENO", "tradeno", "trade_no", "tradeid", "trade_id", "id"]
TIME_KEYS = ["TRADETIME", "tradetime", "trade_time", "systime", "updatetime", "time"]
PRICE_KEYS = ["PRICE", "price"]
QTY_KEYS = ["QUANTITY", "quantity", "qty", "lots"]
VALUE_KEYS = ["VALUE", "value", "trade_value", "tradevalue"]
LOTSIZE_KEYS = ["LOTSIZE", "lotsize", "lot", "lot_size"]
BUYSELL_KEYS = ["BUYSELL", "buysell", "buy_sell", "side", "SIDE"]


@dataclass
class Trade:
    secid: str
    board: str
    market: str
    engine: str
    trade_id: Optional[str]
    trade_time: Optional[str]
    price: Optional[float]
    quantity: Optional[float]
    value: float
    buy_sell: Optional[str] = None  # "B" = Buy, "S" = Sell
    source_index: int = 0

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
        return (self.trade_time or "", self.id_num, self.source_index)

    @property
    def is_buy(self) -> Optional[bool]:
        """True если покупка (BUY), False если продажа (SELL), None если неизвестно."""
        if self.buy_sell is None:
            return None
        return str(self.buy_sell).upper().startswith("B")

    @property
    def direction_emoji(self) -> str:
        if self.buy_sell is None:
            return "⚪"
        if str(self.buy_sell).upper().startswith("B"):
            return "🟢"  # Покупка
        return "🔴"  # Продажа

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
            return f"id:{self.engine}:{self.market}:{self.board}:{self.secid}:{self.trade_id}"
        payload = "|".join([
            self.engine, self.market, self.board, self.secid,
            str(self.trade_time or ""),
            str(self.price if self.price is not None else ""),
            str(self.quantity if self.quantity is not None else ""),
            str(self.value),
            str(self.buy_sell or ""),
        ])
        return "hash:" + hashlib.sha1(payload.encode("utf-8")).hexdigest()


@dataclass
class PaginationState:
    current_start: int = 0
    max_tradeno: int = 0
    edge_found: bool = False
    last_response_count: int = 0


def _pick(row: Dict[str, Any], keys: List[str]) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_table(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if isinstance(payload.get("trades"), dict):
        return payload["trades"]
    for value in payload.values():
        if isinstance(value, dict) and "columns" in value and "data" in value:
            return value
    return {}


class MoexClient:
    INITIAL_START = 200000
    START_STEP = 5000
    MAX_LIMIT = 5000
    MIN_START = 0

    def __init__(self, timeout: float = 10.0):
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=timeout,
            headers={"User-Agent": "moex-large-trades-bot"},
            follow_redirects=True,
        )
        self._pagination: Dict[str, PaginationState] = {}

    async def close(self) -> None:
        await self._client.aclose()

    def _get_state(self, key: str) -> PaginationState:
        if key not in self._pagination:
            self._pagination[key] = PaginationState()
        return self._pagination[key]

    async def _fetch_raw(
        self,
        secid: str,
        engine: str,
        market: str,
        board: str,
        start: int,
        limit: int,
    ) -> Tuple[List[Trade], int]:
        path = (
            f"/engines/{engine}/markets/{market}/boards/{board}"
            f"/securities/{secid}/trades.json"
        )
        params = {
            "iss.meta": "off",
            "iss.only": "trades",
            "limit": limit,
            "start": start,
        }

        try:
            response = await self._client.get(path, params=params)
            response.raise_for_status()
            payload = response.json()
            table = _extract_table(payload)
            columns = table.get("columns", [])
            rows = table.get("data", [])

            trades: List[Trade] = []
            for idx, row in enumerate(rows):
                if not isinstance(row, list):
                    continue
                item = dict(zip(columns, row))

                raw_id = _pick(item, ID_KEYS)
                trade_id = None if raw_id is None or str(raw_id).strip() == "" else str(raw_id)

                raw_time = _pick(item, TIME_KEYS)
                trade_time = None if raw_time is None else str(raw_time)

                price = _to_float(_pick(item, PRICE_KEYS))
                quantity = _to_float(_pick(item, QTY_KEYS))
                raw_value = _to_float(_pick(item, VALUE_KEYS))
                lotsize = _to_float(_pick(item, LOTSIZE_KEYS)) or 1.0

                raw_buysell = _pick(item, BUYSELL_KEYS)
                buy_sell = None if raw_buysell is None else str(raw_buysell).strip() or None

                if raw_value is not None and raw_value > 0:
                    value = abs(raw_value)
                elif price is not None and quantity is not None:
                    value = abs(price) * abs(quantity) * lotsize
                else:
                    value = 0.0

                trades.append(
                    Trade(
                        secid=secid.upper(),
                        board=board,
                        market=market,
                        engine=engine,
                        trade_id=trade_id,
                        trade_time=trade_time,
                        price=price,
                        quantity=quantity,
                        value=value,
                        buy_sell=buy_sell,
                        source_index=idx,
                    )
                )
            return trades, len(trades)
        except Exception as e:
            logger.debug("Fetch error for %s (start=%d): %s", secid, start, e)
            return [], 0

    async def find_edge(
        self,
        secid: str,
        engine: str = "stock",
        market: str = "shares",
        board: str = "TQBR",
    ) -> PaginationState:
        key = f"{engine}:{market}:{board}:{secid}"
        state = self._get_state(key)

        if state.edge_found:
            return state

        logger.info("Finding edge for %s (starting from %d)...", secid, self.INITIAL_START)

        current_start = self.INITIAL_START
        direction = "down"

        for _ in range(60):
            trades, count = await self._fetch_raw(
                secid, engine, market, board,
                start=current_start,
                limit=self.MAX_LIMIT,
            )

            if count == 0:
                if direction == "up":
                    current_start = max(self.MIN_START, current_start - self.START_STEP)
                    direction = "down"
                else:
                    current_start = max(self.MIN_START, current_start - self.START_STEP)

                if current_start <= self.MIN_START:
                    current_start = self.MIN_START
                    trades, count = await self._fetch_raw(
                        secid, engine, market, board,
                        start=current_start,
                        limit=self.MAX_LIMIT,
                    )
                    break

            elif count >= self.MAX_LIMIT:
                direction = "up"
                current_start += self.START_STEP
            else:
                break

        state.current_start = current_start
        state.last_response_count = count

        if trades:
            state.max_tradeno = max(t.id_num for t in trades)
            state.edge_found = True
            logger.info(
                "Edge found for %s: start=%d, trades=%d, max_tradeno=%d",
                secid, current_start, count, state.max_tradeno,
            )
        else:
            logger.warning("No trades found for %s after edge search", secid)

        return state

    async def fetch_new_trades(
        self,
        secid: str,
        engine: str = "stock",
        market: str = "shares",
        board: str = "TQBR",
    ) -> List[Trade]:
        key = f"{engine}:{market}:{board}:{secid}"
        state = self._get_state(key)

        if not state.edge_found:
            await self.find_edge(secid, engine, market, board)
            state = self._get_state(key)
            if not state.edge_found:
                return []

        trades, count = await self._fetch_raw(
            secid, engine, market, board,
            start=state.current_start,
            limit=self.MAX_LIMIT,
        )

        if count == 0:
            return []

        if count >= self.MAX_LIMIT:
            state.current_start += self.MAX_LIMIT
            logger.debug(
                "%s: full table (%d trades), moving start to %d",
                secid, count, state.current_start,
            )
            new_trades = trades
        else:
            new_trades = [t for t in trades if t.id_num > state.max_tradeno]

        if new_trades:
            new_max = max(t.id_num for t in new_trades)
            if new_max > state.max_tradeno:
                state.max_tradeno = new_max

        state.last_response_count = count
        return new_trades

    async def fetch_trades(
        self,
        secid: str,
        engine: str = "stock",
        market: str = "shares",
        board: str = "TQBR",
        limit: int = 500,
        start: int = 0,
    ) -> List[Trade]:
        return await self.fetch_new_trades(secid, engine, market, board)
