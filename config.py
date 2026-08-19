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
    # FIGI будет зарезолвен автоматически при старте
    figi: Optional[str] = None


@dataclass
class AppConfig:
    tinvest_token: str
    bot_token: str
    chat_id: str
    default_min_value_rub: float
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
            logger.warning("TICKERS_JSON is not a list, falling back to file/csv")
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse TICKERS_JSON: %s", e)

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
    tinvest_token = os.getenv("TINVEST_TOKEN", "").strip()
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not tinvest_token:
        raise RuntimeError(
            "TINVEST_TOKEN is not set. "
            "Get it in T-Bank app: Invest -> Settings -> API"
        )
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is not set")
    if not chat_id:
        raise RuntimeError("TELEGRAM_CHAT_ID is not set")

    config_path = os.getenv("CONFIG_PATH", "config.yaml").strip()
    file_cfg = _load_file_config(config_path)

    default_min_value_rub = _to_float(
        os.getenv("DEFAULT_MIN_VALUE_RUB", file_cfg.get("default_min_value_rub", 2_000_000)),
        2_000_000,
    )
    send_start_message = _to_bool(
        os.getenv("SEND_START_MESSAGE", file_cfg.get("send_start_message", False)),
        False,
    )
    max_seen_per_ticker = _to_int(
        os.getenv("MAX_SEEN_PER_TICKER", file_cfg.get("max_seen_per_ticker", 50000)),
        50000,
    )

    default_min_value_rub = max(0.0, default_min_value_rub or 0.0)
    max_seen_per_ticker = max(100, max_seen_per_ticker or 50000)

    raw_tickers = _raw_tickers(file_cfg)
    if not isinstance(raw_tickers, list):
        raise RuntimeError("Tickers config must be a list")

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
                figi=str(item.get("figi", "")).strip() or None,
            )
        )

    # Убираем дубли
    unique: Dict[str, TickerConfig] = {}
    for ticker in tickers:
        if ticker.ticker not in unique:
            unique[ticker.ticker] = ticker
    tickers = list(unique.values())

    if not tickers:
        raise RuntimeError("No tickers configured.")

    return AppConfig(
        tinvest_token=tinvest_token,
        bot_token=bot_token,
        chat_id=chat_id,
        default_min_value_rub=default_min_value_rub,
        send_start_message=send_start_message,
        max_seen_per_ticker=max_seen_per_ticker,
        tickers=tickers,
    )
