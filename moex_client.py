import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

BASE_URL = "https://iss.moex.com/iss"

ID_KEYS = [
    "tradeno",
    "trade_no",
    "tradeid",
    "trade_id",
    "id",
]

TIME_KEYS = [
    "tradetime",
    "trade_time",
    "systime",
    "updatetime",
    "time",
]

PRICE_KEYS = [
    "price",
    "PRICE",
]

QTY_KEYS = [
    "quantity",
    "qty",
    "QUANTITY",
    "lots",
]

VALUE_KEYS = [
    "value",
    "VALUE",
    "trade_value",
    "tradevalue",
]

LOTSIZE_KEYS = [
    "lotsize",
    "lot",
    "LOTSIZE",
    "lot_size",
]


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
        return (
            self.trade_time or "",
            self.id_num,
            self.source_index,
        )

    @property
    def dedup_key(self) -> str:
        if self.trade_id:
            return (
                f"id:{self.engine}:{self.market}:{self.board}:"
                f"{self.secid}:{self.trade_id}"
            )

        payload = "|".join(
            [
                self.engine,
                self.market,
                self.board,
                self.secid,
                str(self.trade_time or ""),
                str(self.price if self.price is not None else ""),
                str(self.quantity if self.quantity is not None else ""),
                str(self.value),
            ]
        )

        return "hash:" + hashlib.sha1(payload.encode("utf-8")).hexdigest()


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
    def __init__(self, timeout: float = 8.0):
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            timeout=timeout,
            headers={
                "User-Agent": "moex-large-trades-bot",
            },
            follow_redirects=True,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_trades(
        self,
        secid: str,
        engine: str = "stock",
        market: str = "shares",
        board: str = "TQBR",
        limit: int = 100,
    ) -> List[Trade]:
        path = (
            f"/engines/{engine}"
            f"/markets/{market}"
            f"/boards/{board}"
            f"/securities/{secid}"
            f"/trades.json"
        )

        params = {
            "iss.meta": "off",
            "iss.only": "trades",
            "limit": limit,
        }

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

            computed_value = 0.0

            if price is not None and quantity is not None:
                computed_value = abs(price) * abs(quantity) * lotsize

            if raw_value is not None and raw_value > 0:
                value = abs(raw_value)
            else:
                value = computed_value

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
                    source_index=idx,
                )
            )

        trades.sort(key=lambda trade: trade.sort_key)
        return trades
