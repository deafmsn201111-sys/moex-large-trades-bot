import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class TickerConfig:
    ticker: str
    min_value_rub: Optional[float] = None
    board: str = "TQBR"
    market: str = "shares"
    engine: str = "stock"


@dataclass
class AppConfig:
    bot_token: str
    chat_id: str
    poll_seconds: float
    request_limit: int
    concurrency: int
    default_min_value_rub: float
    send_history_on_start: bool
    send_start_message: bool
    max_seen_per_ticker: int
    tickers: List[TickerConfig]


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _to_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _load_file_config(path: str) -> Dict[str, Any]:
    if not path or not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        return {}
    return data


def _raw_tickers(file_cfg: Dict[str, Any]) -> List[Any]:
    env_json = os.getenv("TICKERS_JSON", "").strip()
    if env_json:
        try:
            parsed = json.loads(env_json)
            if isinstance(parsed, list):
                return parsed
            else:
                logger.warning("TICKERS_JSON is not a list, falling back to file/csv")
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse TICKERS_JSON: %s. Falling back to file/csv.", e)

    if file_cfg.get("tickers"):
        return file_cfg.get("tickers") or []

    env_csv = os.getenv("TICKERS", "").strip()
    if env_csv:
        return [
            {"ticker": item.strip()}
            for item in env_csv.split(",")
            if item.strip()
        ]

    return []


def load_config() -> AppConfig:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set")
    if not chat_id:
        raise RuntimeError("TELEGRAM_CHAT_ID is not set")

    config_path = os.getenv("CONFIG_PATH", "config.yaml").strip()
    file_cfg = _load_file_config(config_path)

    poll_seconds = _to_float(
        os.getenv("POLL_SECONDS", file_cfg.get("poll_seconds", 1.0)), 1.0
    )
    request_limit = _to_int(
        os.getenv("REQUEST_LIMIT", file_cfg.get("request_limit", 100)), 100
    )
    concurrency = _to_int(
        os.getenv("CONCURRENCY", file_cfg.get("concurrency", 5)), 5
    )
    default_min_value_rub = _to_float(
        os.getenv("DEFAULT_MIN_VALUE_RUB", file_cfg.get("default_min_value_rub", 1_000_000)),
        1_000_000,
    )
    send_history_on_start = _to_bool(
        os.getenv("SEND_HISTORY_ON_START", file_cfg.get("send_history_on_start", False)),
        False,
    )
    send_start_message = _to_bool(
        os.getenv("SEND_START_MESSAGE", file_cfg.get("send_start_message", False)),
        False,
    )
    max_seen_per_ticker = _to_int(
        os.getenv("MAX_SEEN_PER_TICKER", file_cfg.get("max_seen_per_ticker", 10000)),
        10000,
    )

    poll_seconds = max(0.2, poll_seconds or 1.0)
    request_limit = min(max(10, request_limit or 100), 5000)
    concurrency = min(max(1, concurrency or 1), 20)
    default_min_value_rub = max(0.0, default_min_value_rub or 0.0)
    max_seen_per_ticker = max(100, max_seen_per_ticker or 10000)

    raw_tickers = _raw_tickers(file_cfg)
    if not isinstance(raw_tickers, list):
        raise RuntimeError("Tickers config must be a list")

    default_board = str(file_cfg.get("board", "TQBR")).strip() or "TQBR"
    default_market = str(file_cfg.get("market", "shares")).strip() or "shares"
    default_engine = str(file_cfg.get("engine", "stock")).strip() or "stock"

    tickers: List[TickerConfig] = []
    for item in raw_tickers:
        if isinstance(item, str):
            item = {"ticker": item}
        if not isinstance(item, dict):
            continue

        ticker = str(item.get("ticker", "")).strip().upper()
        if not ticker:
            continue

        min_value = _to_float(item.get("min_value_rub", item.get("min_value")), None)
        if min_value is not None and min_value < 0:
            min_value = None

        tickers.append(
            TickerConfig(
                ticker=ticker,
                min_value_rub=min_value,
                board=str(item.get("board", default_board)).strip() or default_board,
                market=str(item.get("market", default_market)).strip() or default_market,
                engine=str(item.get("engine", default_engine)).strip() or default_engine,
            )
        )

    unique_tickers: Dict[str, TickerConfig] = {}
    for ticker in tickers:
        key = f"{ticker.engine}:{ticker.market}:{ticker.board}:{ticker.ticker}"
        if key not in unique_tickers:
            unique_tickers[key] = ticker
    tickers = list(unique_tickers.values())

    if not tickers:
        raise RuntimeError("No tickers configured.")

    return AppConfig(
        bot_token=bot_token,
        chat_id=chat_id,
        poll_seconds=poll_seconds,
        request_limit=request_limit,
        concurrency=concurrency,
        default_min_value_rub=default_min_value_rub,
        send_history_on_start=send_history_on_start,
        send_start_message=send_start_message,
        max_seen_per_ticker=max_seen_per_ticker,
        tickers=tickers,
    )
