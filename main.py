import atexit
import base64
import itertools
import json
import logging
import os
import shutil
import sqlite3
import threading
import time
from collections import deque
from datetime import datetime, time as dt_time, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from dhanhq import dhanhq
from flask import Flask, jsonify, request

from depth_shadow import (
    CONFIG as DEPTH_CONFIG,
    DepthFeedManager,
    entry_gate,
    rank_signal,
    watchdog_tick,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")


def _env_time(name: str, default: str) -> dt_time:
    raw = os.getenv(name, default)
    try:
        return datetime.strptime(raw, "%H:%M").time()
    except ValueError:
        log.warning("Invalid %s=%r; falling back to %s", name, raw, default)
        return datetime.strptime(default, "%H:%M").time()

MAIN_CLIENT_ID = os.getenv("MAIN_CLIENT_ID") or os.getenv("DHAN_CLIENT_ID") or "1105120853"
MAIN_ACCESS_TOKEN = os.getenv(
    "MAIN_ACCESS_TOKEN",
    os.getenv("DHAN_ACCESS_TOKEN")
    or "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4Njk1OTE4LCJpYXQiOjE3Nzg2MDk1MTgsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.ryp89RF2HByisq6xld12NprBtOzYpNYQrkxen3Bdxq9zfvH8pJejPNd61ZJ-fxUaOP09w13dBIkbA9s6up6LgA",
)
SANDBOX_CLIENT_ID = os.getenv("SANDBOX_CLIENT_ID", "2605019607")
SANDBOX_ACCESS_TOKEN = os.getenv(
    "SANDBOX_ACCESS_TOKEN",
    "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJwYXJ0bmVySWQiOiIiLCJkaGFuQ2xpZW50SWQiOiIyNjA1MDE5NjA3Iiwid2ViaG9va1VybCI6IiIsImlzcyI6ImRoYW4iLCJleHAiOjE3Nzg1OTg2MjV9.51ENYq_S8LqRQdJ8QEGstmnZPa5zvxhxBofGEVqW3tkXLjnIkchHVmwial5HM7hkO5fA7YIeo1ZzuxMT9pbmsA",
)


def _jwt_meta(token: str, configured_client_id: str, env_name: str) -> dict:
    meta = {
        "env_name": env_name,
        "configured_client_id": configured_client_id,
        "from_env": bool(os.getenv(env_name)),
        "present": bool(token),
    }
    try:
        payload = token.split(".")[1]
        padded = payload + "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded))
        exp = data.get("exp")
        meta.update(
            {
                "token_client_id": str(data.get("dhanClientId") or ""),
                "issuer": data.get("iss"),
                "consumer_type": data.get("tokenConsumerType"),
                "expires_at_ist": datetime.fromtimestamp(exp, IST).isoformat() if exp else None,
                "expired": datetime.now(IST).timestamp() >= exp if exp else None,
            }
        )
    except Exception as exc:
        meta.update({"decode_error": str(exc), "expired": None})
    return meta


TRADING_MODE = os.getenv("TRADING_MODE", "SANDBOX").upper()
if os.getenv("FORCE_SANDBOX_ORDERS", "true").lower() == "true":
    TRADING_MODE = "SANDBOX"
ORDER_FALLBACK_TO_PAPER = os.getenv("ORDER_FALLBACK_TO_PAPER", "true").lower() == "true"
DEPTH_SHADOW_ENABLED = os.getenv("DEPTH_SHADOW_ENABLED", "true").lower() == "true"
STRATEGY_MODE = os.getenv("STRATEGY_MODE", "SUPREME_RUNNER_V2").upper()
FINAL_1M_STRATEGY_ENABLED = os.getenv("FINAL_1M_STRATEGY_ENABLED", "true").lower() == "true"
ADMIN_RESET_TOKEN = os.getenv("ADMIN_RESET_TOKEN", "")


def _resolve_log_dir() -> tuple[str, str | None]:
    explicit_log_dir = os.getenv("LOG_DIR")
    if explicit_log_dir:
        return explicit_log_dir, os.getenv("RAILWAY_VOLUME_MOUNT_PATH")

    volume_root = os.getenv("RAILWAY_VOLUME_MOUNT_PATH")
    if not volume_root and os.path.isdir("/data"):
        volume_root = "/data"

    if volume_root:
        return os.path.join(volume_root, "runtime_logs"), volume_root

    return os.path.join(os.path.dirname(__file__), "runtime_logs"), None


LOG_DIR, RAILWAY_VOLUME_ROOT = _resolve_log_dir()
STATE_FILE = os.path.join(LOG_DIR, "runtime_state.json")
EVENT_DB_FILE = os.path.join(LOG_DIR, "runtime_events.sqlite3")
MAX_RECENT_SHADOW_EVENTS = 500
STATE_SAVE_DEBOUNCE_SECS = float(os.getenv("STATE_SAVE_DEBOUNCE_SECS", "1.0"))
QUOTE_CACHE_TTL_SECS = float(os.getenv("QUOTE_CACHE_TTL_SECS", "1.0"))
CANDLE_CACHE_TTL_SECS = float(os.getenv("CANDLE_CACHE_TTL_SECS", "12.0"))

BLACKLIST = {"MEESHO", "MEESHO-BE"}
MAX_SLOTS = 8
SLOT_CAPITAL = 50000
ROLLING_WINDOW_MINS = 25
TOP_1M = 15
TOP_3M = 20
MASTER_SIZE = 15
SMART_ENTRY_MINS = 3
MAX_RED_CANDLE_PCT = 0.008
SL_PCT = float(os.getenv("SL_PCT", "0.010"))
TSL_TRIGGER_PCT = float(os.getenv("TSL_TRIGGER_PCT", "0.025"))
TSL_TRAIL_PCT = float(os.getenv("TSL_TRAIL_PCT", "0.020"))
TIME_EXIT_MINS = int(os.getenv("TIME_EXIT_MINS", "45"))
WARMUP_END = _env_time("TRADING_START_TIME", "09:25")
EOD_EXIT_TIME = dt_time(15, 29)
MASTER_FILE = "master_list.json"
SHADOW_SNAPSHOT_LOG_SECS = int(os.getenv("SHADOW_SNAPSHOT_LOG_SECS", "15"))
RUNNER_OVERRIDE_SCORE = int(os.getenv("RUNNER_OVERRIDE_SCORE", "40"))
RUNNER_MODE_SCORE = int(os.getenv("RUNNER_MODE_SCORE", "10000"))
RUNNER_STALL_MINS = int(os.getenv("RUNNER_STALL_MINS", "90"))
RUNNER_STALL_PEAK_PCT = float(os.getenv("RUNNER_STALL_PEAK_PCT", "0.04"))
RUNNER_TSL_TRIGGER_PCT = float(os.getenv("RUNNER_TSL_TRIGGER_PCT", "0.025"))
RUNNER_TSL_TRAIL_PCT = float(os.getenv("RUNNER_TSL_TRAIL_PCT", "0.025"))
ENTRY_CLOSE_POSITION_MIN = float(os.getenv("ENTRY_CLOSE_POSITION_MIN", "0.75"))
ENTRY_VOLUME_CURR10_MIN = float(os.getenv("ENTRY_VOLUME_CURR10_MIN", "1.05"))
SUPERTREND_EXIT_ENABLED = os.getenv("SUPERTREND_EXIT_ENABLED", "true").lower() == "true"
SUPERTREND_ATR_PERIOD = int(os.getenv("SUPERTREND_ATR_PERIOD", "7"))
SUPERTREND_MULTIPLIER = float(os.getenv("SUPERTREND_MULTIPLIER", "3.0"))

pool_1m: dict[str, list[datetime]] = {}
pool_3m: dict[str, list[datetime]] = {}
pool_5m: dict[str, list[datetime]] = {}
pool_buy: dict[str, list[datetime]] = {}
volumes: dict[str, int] = {}
master: list[str] = []
positions: dict[str, dict] = {}
shadow_positions: dict[str, dict] = {}
pending: dict[str, dict] = {}
trades_today: list[dict] = []
shadow_trades: list[dict] = []
state_lock = threading.RLock()
log_lock = threading.RLock()
watchdog_started = False
order_counter = itertools.count(1)
state_dirty = False
last_state_save_ts = 0.0
quote_cache: dict[str, dict] = {}
candle_cache: dict[str, dict] = {}
shadow_recent_events = deque(maxlen=MAX_RECENT_SHADOW_EVENTS)

app = Flask(__name__)

os.makedirs(LOG_DIR, exist_ok=True)


def _now() -> datetime:
    return datetime.now(IST)


def _today_tag() -> str:
    return _now().strftime("%Y%m%d")


def _make_id(prefix: str) -> str:
    return f"{prefix}_{_now().strftime('%Y%m%d_%H%M%S')}_{next(order_counter):05d}"


def _init_event_db():
    try:
        with sqlite3.connect(EVENT_DB_FILE) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_stream_ts ON events(stream, ts DESC)"
            )
            conn.commit()
    except Exception as exc:
        log.warning("Failed to initialize event db: %s", exc)


def _mark_state_dirty():
    global state_dirty
    state_dirty = True


def _atomic_write_json(path: str, payload: dict):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)
    os.replace(tmp_path, path)


def _append_event(stream: str, event_type: str, payload: dict):
    event = {"ts": _now().isoformat(), "stream": stream, "event": event_type, **_serialize_value(payload)}
    path = os.path.join(LOG_DIR, f"{stream}_{_today_tag()}.jsonl")
    with log_lock:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True) + "\n")
        try:
            with sqlite3.connect(EVENT_DB_FILE) as conn:
                conn.execute(
                    "INSERT INTO events(ts, stream, event_type, payload_json) VALUES (?, ?, ?, ?)",
                    (event["ts"], stream, event_type, json.dumps(event, ensure_ascii=True)),
                )
                conn.commit()
        except Exception as exc:
            log.warning("Failed to append event to sqlite: %s", exc)
    if stream == "shadow":
        shadow_recent_events.append(event)
    return event


def _serialize_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _serialize_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    return value


def _parse_dt(value):
    if isinstance(value, datetime):
        return value
    return _parse_candle_time(value)


def _load_recent_shadow_events():
    if not os.path.exists(EVENT_DB_FILE):
        return
    try:
        with sqlite3.connect(EVENT_DB_FILE) as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM events
                WHERE stream = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                ("shadow", MAX_RECENT_SHADOW_EVENTS),
            ).fetchall()
        shadow_recent_events.clear()
        for (payload_json,) in reversed(rows):
            shadow_recent_events.append(json.loads(payload_json))
    except Exception as exc:
        log.warning("Failed to restore recent shadow events: %s", exc)


def _query_event_rows(stream: str | None = None, event_type: str | None = None, day: str | None = None, limit: int = 200):
    if not os.path.exists(EVENT_DB_FILE):
        return []

    clauses = []
    params = []
    if stream:
        clauses.append("stream = ?")
        params.append(stream)
    if event_type:
        clauses.append("event_type = ?")
        params.append(event_type)
    if day:
        clauses.append("ts LIKE ?")
        params.append(f"{day}%")

    sql = "SELECT payload_json FROM events"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(max(1, min(int(limit), 5000)))

    try:
        with sqlite3.connect(EVENT_DB_FILE) as conn:
            rows = conn.execute(sql, params).fetchall()
        return [json.loads(payload_json) for (payload_json,) in reversed(rows)]
    except Exception as exc:
        log.warning("Failed to query events: %s", exc)
        return []


def _filter_trades_by_day(trades: list[dict], day: str | None) -> list[dict]:
    if not day:
        return list(trades)
    prefix = f"{day}T"
    return [
        trade
        for trade in trades
        if str(trade.get("entry_time", "")).startswith(prefix) or str(trade.get("exit_time", "")).startswith(prefix)
    ]


def _parse_candle_time(raw) -> datetime | None:
    if raw in (None, "", 0):
        return None

    try:
        if isinstance(raw, (int, float)):
            ts = float(raw)
            if ts > 1_000_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, IST)

        if isinstance(raw, str):
            txt = raw.strip().replace("Z", "+00:00")
            if txt.isdigit():
                return _parse_candle_time(int(txt))
            dt = datetime.fromisoformat(txt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=IST)
            return dt.astimezone(IST)
    except Exception:
        return None

    return None


def _parse_chartink_time(raw) -> datetime | None:
    if raw in (None, ""):
        return None
    if isinstance(raw, (int, float)):
        return _parse_candle_time(raw)

    text = str(raw).strip()
    if not text:
        return None

    normalized = " ".join(text.replace(",", " ").split())
    formats = [
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %I:%M:%S %p",
        "%d-%m-%Y %I:%M %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return _parse_candle_time(text)


def _floor_minute(value: datetime) -> datetime:
    return value.astimezone(IST).replace(second=0, microsecond=0)


def _alert_signal_cutoff(now: datetime | None = None, payload_time: datetime | None = None) -> datetime:
    if payload_time is not None:
        return _floor_minute(payload_time)
    # Chartink 1m alerts arrive after the candle closes. Anchor the signal to
    # the prior completed minute instead of Dhan's possibly in-progress candle.
    return _floor_minute(now or _now()) - timedelta(minutes=1)


def _candles_until(candles: list[dict], cutoff: datetime) -> list[dict]:
    selected = []
    for candle in candles or []:
        candle_time = _parse_candle_time(candle.get("time"))
        if candle_time is not None and candle_time <= cutoff:
            selected.append(candle)
    return selected


def _select_alert_signal_candle(
    candles: list[dict],
    now: datetime | None = None,
    payload_time: datetime | None = None,
) -> tuple[dict | None, datetime | None, datetime, str]:
    cutoff = _alert_signal_cutoff(now, payload_time=payload_time)
    selected = _candles_until(candles, cutoff)
    if selected:
        candle = selected[-1]
        source = "PAYLOAD_SIGNAL_TIME" if payload_time is not None else "ALERT_PREVIOUS_COMPLETED_1M"
        return candle, _parse_candle_time(candle.get("time")), cutoff, source

    # If Dhan omits timestamps or returns an unusual payload, avoid using the
    # freshest candle as the signal candle during market hours.
    if len(candles or []) >= 2:
        candle = candles[-2]
        return candle, _parse_candle_time(candle.get("time")), cutoff, "FALLBACK_PREVIOUS_INDEX"
    if candles:
        candle = candles[-1]
        return candle, _parse_candle_time(candle.get("time")), cutoff, "FALLBACK_ONLY_CANDLE"
    return None, None, cutoff, "NO_CANDLE"


def _payload_items_from_request() -> list[dict]:
    payload = request.get_json(silent=True) or {}
    raw = payload.get("stocks") or payload.get("data") or payload.get("rows") or payload.get("results")
    if raw is None and any(key in payload for key in ("symbol", "Symbol", "SYMBOL", "nsecode", "NSE Code", "stock", "Stock", "ticker", "Ticker")):
        return [payload]
    if isinstance(raw, str):
        shared_fields = {
            key: payload.get(key)
            for key in (
                "date",
                "Date",
                "DATE",
                "datetime",
                "Datetime",
                "timestamp",
                "Timestamp",
                "time",
                "Time",
                "scan_time",
                "triggered_at",
                "trigger_time",
            )
            if payload.get(key) not in (None, "")
        }
        return [{"symbol": item.strip(), **shared_fields} for item in raw.split(",") if item.strip()]
    if isinstance(raw, dict):
        return [raw]
    if isinstance(raw, list):
        return [{"symbol": item} if not isinstance(item, dict) else item for item in raw]
    return []


def _symbol_from_payload_item(item: dict) -> str:
    for key in ("symbol", "Symbol", "SYMBOL", "nsecode", "NSE Code", "stock", "Stock", "ticker", "Ticker"):
        value = item.get(key)
        if value:
            return str(value).strip().upper()
    return ""


def _timestamp_from_payload_item(item: dict) -> datetime | None:
    for key in (
        "date",
        "Date",
        "DATE",
        "datetime",
        "Datetime",
        "timestamp",
        "Timestamp",
        "time",
        "Time",
        "scan_time",
        "triggered_at",
        "trigger_time",
    ):
        value = item.get(key)
        parsed = _parse_chartink_time(value)
        if parsed is not None:
            return parsed
    return None


def _parse_signal_items_from_request() -> list[dict]:
    items = []
    seen = set()
    for item in _payload_items_from_request():
        symbol = _symbol_from_payload_item(item)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        items.append({"symbol": symbol, "payload_time": _timestamp_from_payload_item(item), "raw": item})
    return items


def _parse_symbols_from_request() -> list[str]:
    return [item["symbol"] for item in _parse_signal_items_from_request()]


def _request_signal_time_by_symbol() -> dict[str, datetime]:
    return {
        item["symbol"]: item["payload_time"]
        for item in _parse_signal_items_from_request()
        if item.get("payload_time") is not None
    }


def _pool_to_json(pool: dict[str, list[datetime]]) -> dict[str, list[str]]:
    return {symbol: [stamp.isoformat() for stamp in stamps] for symbol, stamps in pool.items()}


def _pool_from_json(raw: dict) -> dict[str, list[datetime]]:
    parsed = {}
    for symbol, items in (raw or {}).items():
        stamps = []
        for item in items or []:
            stamp = _parse_dt(item)
            if stamp is not None:
                stamps.append(stamp)
        if stamps:
            parsed[str(symbol).upper()] = stamps
    return parsed


def _position_from_json(raw: dict) -> dict:
    data = dict(raw or {})
    for key in ("entry_time",):
        if key in data:
            data[key] = _parse_dt(data.get(key))
    return data


def _pending_from_json(raw: dict) -> dict:
    data = dict(raw or {})
    for key in ("queued_at", "expires_at", "signal_candle_time", "last_candle_check"):
        if key in data:
            data[key] = _parse_dt(data.get(key))
    return data


def _save_runtime_state(force: bool = False):
    global state_dirty, last_state_save_ts

    if not force and not state_dirty:
        return

    now_ts = time.time()
    if not force and (now_ts - last_state_save_ts) < STATE_SAVE_DEBOUNCE_SECS:
        return

    with state_lock:
        payload = {
            "saved_at": _now().isoformat(),
            "master": list(master),
            "pool_1m": _pool_to_json(pool_1m),
            "pool_3m": _pool_to_json(pool_3m),
            "pool_5m": _pool_to_json(pool_5m),
            "pool_buy": _pool_to_json(pool_buy),
            "volumes": dict(volumes),
            "positions": _serialize_value(positions),
            "shadow_positions": _serialize_value(shadow_positions),
            "pending": _serialize_value(pending),
            "trades_today": _serialize_value(trades_today[-500:]),
            "shadow_trades": _serialize_value(shadow_trades[-500:]),
        }

    try:
        _atomic_write_json(STATE_FILE, payload)
        last_state_save_ts = now_ts
        state_dirty = False
    except Exception as exc:
        log.warning("Failed to save runtime state: %s", exc)


def _load_runtime_state():
    global master, volumes

    if not os.path.exists(STATE_FILE):
        return

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        log.warning("Failed to load runtime state: %s", exc)
        return

    with state_lock:
        master[:] = [str(item).upper() for item in payload.get("master", [])]
        volumes.clear()
        volumes.update({str(k).upper(): int(v or 0) for k, v in (payload.get("volumes") or {}).items()})

        pool_1m.clear()
        pool_1m.update(_pool_from_json(payload.get("pool_1m") or {}))
        pool_3m.clear()
        pool_3m.update(_pool_from_json(payload.get("pool_3m") or {}))
        pool_5m.clear()
        pool_5m.update(_pool_from_json(payload.get("pool_5m") or {}))
        pool_buy.clear()
        pool_buy.update(_pool_from_json(payload.get("pool_buy") or {}))

        positions.clear()
        positions.update(
            {
                str(symbol).upper(): _position_from_json(pos)
                for symbol, pos in (payload.get("positions") or {}).items()
            }
        )
        shadow_positions.clear()
        shadow_positions.update(
            {
                str(symbol).upper(): _position_from_json(pos)
                for symbol, pos in (payload.get("shadow_positions") or {}).items()
            }
        )
        pending.clear()
        pending.update(
            {
                str(symbol).upper(): _pending_from_json(pos)
                for symbol, pos in (payload.get("pending") or {}).items()
            }
        )
        trades_today.clear()
        trades_today.extend(payload.get("trades_today") or [])
        shadow_trades.clear()
        shadow_trades.extend(payload.get("shadow_trades") or [])

    log.info(
        "Restored runtime state: master=%s pending=%s baseline_open=%s shadow_open=%s",
        len(master),
        len(pending),
        len(positions),
        len(shadow_positions),
    )


def _archive_file(path: str, label: str) -> str | None:
    if not os.path.exists(path):
        return None

    archive_dir = os.path.join(LOG_DIR, "archives")
    os.makedirs(archive_dir, exist_ok=True)
    stamp = _now().strftime("%Y%m%d_%H%M%S")
    archive_path = os.path.join(archive_dir, f"{stamp}_{label}_{os.path.basename(path)}")
    shutil.move(path, archive_path)
    return archive_path


def _reset_runtime_state(reason: str = "manual_reset") -> dict:
    archived_state = _archive_file(STATE_FILE, "pre_reset")
    archived_master = _archive_file(MASTER_FILE, "pre_reset")
    with state_lock:
        master.clear()
        pool_1m.clear()
        pool_3m.clear()
        pool_5m.clear()
        pool_buy.clear()
        volumes.clear()
        positions.clear()
        shadow_positions.clear()
        pending.clear()
        trades_today.clear()
        shadow_trades.clear()
        shadow_recent_events.clear()
        _mark_state_dirty()
    _save_runtime_state(force=True)
    payload = {
        "reason": reason,
        "archived_state": archived_state,
        "archived_master": archived_master,
        "reset_at": _now().isoformat(),
    }
    _append_event("baseline", "runtime_reset", payload)
    _append_event("shadow", "runtime_reset", payload)
    return payload


def _save_master():
    try:
        with open(MASTER_FILE, "w", encoding="utf-8") as handle:
            json.dump(master, handle)
    except Exception as exc:
        log.warning("Failed to save master list: %s", exc)


def _load_master():
    global master
    try:
        if os.path.exists(MASTER_FILE):
            with open(MASTER_FILE, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, list):
                master = [str(item).upper() for item in data]
                log.info("Restored master list with %s symbols.", len(master))
    except Exception as exc:
        log.warning("Failed to load master list: %s", exc)


def _ensure_dhan_data():
    global dhan_data
    if dhan_data is None:
        try:
            dhan_data = dhanhq(client_id=MAIN_CLIENT_ID, access_token=MAIN_ACCESS_TOKEN)
            log.info("Dhan market data client initialized.")
        except Exception as exc:
            log.error("Dhan data client init failed: %s", exc, exc_info=True)
            dhan_data = None


dhan_data = None
_ensure_dhan_data()

try:
    dhan_orders = dhanhq(client_id=SANDBOX_CLIENT_ID, access_token=SANDBOX_ACCESS_TOKEN)
    if hasattr(dhan_orders, "dhan_http"):
        dhan_orders.dhan_http.base_url = "https://sandbox.dhan.co/v2"
    log.info("Dhan sandbox orders client initialized.")
except Exception as exc:
    log.warning("Dhan sandbox orders init failed: %s", exc)
    dhan_orders = None


def _ensure_dhan_orders():
    global dhan_orders
    if dhan_orders is not None:
        return
    try:
        dhan_orders = dhanhq(client_id=SANDBOX_CLIENT_ID, access_token=SANDBOX_ACCESS_TOKEN)
        if hasattr(dhan_orders, "dhan_http"):
            dhan_orders.dhan_http.base_url = "https://sandbox.dhan.co/v2"
        log.info("Dhan sandbox orders client restored.")
    except Exception as exc:
        log.warning("Dhan sandbox orders restore failed: %s", exc)
        dhan_orders = None


def _paper_fallback_order(symbol: str, qty: int, side: str, trade_id: str, message: str) -> dict:
    fallback = {
        "status": "success",
        "mode": "PAPER_FALLBACK",
        "orderId": _make_id("paper_fallback_order"),
        "symbol": symbol,
        "side": side,
        "quantity": qty,
        "trade_id": trade_id,
        "placed_at": _now().isoformat(),
        "sandbox_error": message,
    }
    _append_event("baseline", "sandbox_order_fallback", fallback)
    return fallback


_SECURITY_MAP: dict[str, str] = {}
try:
    map_path = os.path.join(os.path.dirname(__file__), "security_map.csv")
    if os.path.exists(map_path):
        df = pd.read_csv(map_path, low_memory=False)
        df = df[df["SEM_INSTRUMENT_NAME"] == "EQUITY"]
        for _, row in df.iterrows():
            _SECURITY_MAP[str(row["SEM_TRADING_SYMBOL"]).upper()] = str(row["SEM_SMST_SECURITY_ID"])
        log.info("Security map loaded with %s equity symbols.", len(_SECURITY_MAP))
except Exception as exc:
    log.warning("Security map load failed: %s", exc)


def _resolve_security_id(symbol: str) -> str:
    return _SECURITY_MAP.get(symbol.upper(), symbol)


depth_manager = DepthFeedManager(
    client_id=MAIN_CLIENT_ID,
    access_token=MAIN_ACCESS_TOKEN,
    security_resolver=_resolve_security_id,
    enabled=DEPTH_SHADOW_ENABLED,
)


def _get_quote_snapshot(symbol: str) -> dict | None:
    cache_key = symbol.upper()
    cached = quote_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) <= QUOTE_CACHE_TTL_SECS:
        return dict(cached["quote"])

    try:
        _ensure_dhan_data()
        if dhan_data is None:
            return None

        sec_id = _resolve_security_id(symbol)
        if not str(sec_id).isdigit():
            return None

        resp = dhan_data.quote_data({"NSE_EQ": [int(sec_id)]})
        if resp.get("status") != "success":
            return None

        quote = resp["data"]["data"]["NSE_EQ"].get(str(sec_id), {})
        quote["security_id"] = str(sec_id)
        quote_cache[cache_key] = {"ts": time.time(), "quote": dict(quote)}
        return quote
    except Exception:
        return None


def _get_ltp(symbol: str) -> float | None:
    quote = _get_quote_snapshot(symbol)
    if not quote:
        return None
    last_price = quote.get("last_price")
    return float(last_price) if last_price is not None else None


def _get_bulk_volumes(symbols: list[str]):
    _ensure_dhan_data()
    if not symbols or dhan_data is None:
        return

    try:
        unique = []
        id_to_symbol = {}
        seen = set()
        for symbol in symbols:
            if symbol in seen:
                continue
            seen.add(symbol)
            sec_id = _resolve_security_id(symbol)
            if str(sec_id).isdigit():
                unique.append(int(sec_id))
                id_to_symbol[str(sec_id)] = symbol

        for idx in range(0, len(unique), 25):
            chunk = unique[idx:idx + 25]
            resp = dhan_data.quote_data({"NSE_EQ": chunk})
            if resp.get("status") != "success":
                continue
            quote_map = resp["data"]["data"]["NSE_EQ"]
            for sec_id, quote in quote_map.items():
                symbol = id_to_symbol.get(sec_id)
                if symbol:
                    volumes[symbol] = int(quote.get("volume", 0) or 0)
    except Exception as exc:
        log.warning("Bulk volume fetch failed: %s", exc)


def _get_1min_candles(symbol: str, n: int = 35) -> list[dict] | None:
    cache_key = symbol.upper()
    cached = candle_cache.get(cache_key)
    if cached and (time.time() - cached["ts"]) <= CANDLE_CACHE_TTL_SECS:
        candles = cached["candles"]
        return candles[-n:] if candles else None

    try:
        _ensure_dhan_data()
        if dhan_data is None:
            return None

        sec_id = _resolve_security_id(symbol)
        if not str(sec_id).isdigit():
            return None

        today = _now().strftime("%Y-%m-%d")
        resp = dhan_data.intraday_minute_data(sec_id, "NSE_EQ", "EQUITY", today, today)
        if resp.get("status") != "success" or not resp.get("data"):
            return None

        data = resp["data"]
        length = len(data.get("close", []))
        if length == 0:
            return None

        ts_key = "timestamp"
        if ts_key not in data:
            ts_key = "start_Time" if "start_Time" in data else "start_time"

        candles = []
        for idx in range(length):
            candles.append(
                {
                    "open": float(data["open"][idx]),
                    "high": float(data["high"][idx]),
                    "low": float(data["low"][idx]),
                    "close": float(data["close"][idx]),
                    "volume": int(data.get("volume", [0] * length)[idx] or 0),
                    "time": data.get(ts_key, [0] * length)[idx],
                }
            )
        candle_cache[cache_key] = {"ts": time.time(), "candles": candles}
        return candles[-n:]
    except Exception as exc:
        log.warning("1m candle fetch failed for %s: %s", symbol, exc)
        return None


def _get_recent_30min_volume(symbol: str) -> int:
    candles = _get_1min_candles(symbol, n=35)
    if not candles:
        return 0
    return int(sum(candle.get("volume", 0) for candle in candles[-30:]))


def _volume_curr10(candles: list[dict], idx: int) -> float | None:
    if idx <= 0:
        return None
    prev = candles[max(0, idx - 10):idx]
    if not prev:
        return None
    avg = sum(int(candle.get("volume", 0) or 0) for candle in prev) / len(prev)
    if avg <= 0:
        return None
    return int(candles[idx].get("volume", 0) or 0) / avg


def _candle_close_position(candle: dict) -> float | None:
    high = float(candle.get("high", 0) or 0)
    low = float(candle.get("low", 0) or 0)
    close = float(candle.get("close", 0) or 0)
    span = high - low
    if span <= 0:
        return None
    return (close - low) / span


def _true_ranges(candles: list[dict]) -> list[float]:
    ranges = []
    prev_close = None
    for candle in candles:
        high = float(candle.get("high", 0) or 0)
        low = float(candle.get("low", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        if prev_close is None:
            true_range = high - low
        else:
            true_range = max(high - low, abs(high - prev_close), abs(low - prev_close))
        ranges.append(max(0.0, true_range))
        prev_close = close
    return ranges


def _is_supertrend_bearish(symbol: str) -> tuple[bool, dict]:
    candles = _get_1min_candles(symbol, n=max(80, SUPERTREND_ATR_PERIOD + 20)) or []
    if len(candles) < SUPERTREND_ATR_PERIOD + 2:
        return False, {"reason": "INSUFFICIENT_CANDLES", "candles": len(candles)}

    ranges = _true_ranges(candles)
    final_upper = None
    final_lower = None
    prev_upper = None
    prev_lower = None
    prev_close = None
    bullish = True
    line = None

    for idx, candle in enumerate(candles):
        high = float(candle.get("high", 0) or 0)
        low = float(candle.get("low", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        if idx + 1 < SUPERTREND_ATR_PERIOD:
            prev_close = close
            continue

        atr_window = ranges[idx - SUPERTREND_ATR_PERIOD + 1:idx + 1]
        atr = sum(atr_window) / len(atr_window)
        hl2 = (high + low) / 2
        basic_upper = hl2 + SUPERTREND_MULTIPLIER * atr
        basic_lower = hl2 - SUPERTREND_MULTIPLIER * atr

        if prev_upper is None or prev_lower is None:
            final_upper = basic_upper
            final_lower = basic_lower
            bullish = close >= final_lower
        else:
            final_upper = basic_upper if (basic_upper < prev_upper or prev_close > prev_upper) else prev_upper
            final_lower = basic_lower if (basic_lower > prev_lower or prev_close < prev_lower) else prev_lower
            if bullish and close < final_lower:
                bullish = False
            elif not bullish and close > final_upper:
                bullish = True

        line = final_lower if bullish else final_upper
        prev_upper = final_upper
        prev_lower = final_lower
        prev_close = close

    return not bullish, {
        "period": SUPERTREND_ATR_PERIOD,
        "multiplier": SUPERTREND_MULTIPLIER,
        "line": round(line, 2) if line is not None else None,
        "bullish": bullish,
    }


def _get_daily_turnover_cr(symbol: str, quote: dict | None = None) -> float:
    quote = quote or _get_quote_snapshot(symbol)
    if not quote:
        return 0.0
    volume = int(quote.get("volume", 0) or 0)
    avg_price = float(quote.get("avg_price", 0) or 0)
    last_price = float(quote.get("last_price", 0) or 0)
    price = avg_price if avg_price > 0 else last_price
    if price <= 0 or volume <= 0:
        return 0.0
    return round((price * volume) / 1e7, 2)


def _place_order(symbol: str, qty: int, side: str, trade_id: str):
    if TRADING_MODE == "PAPER":
        return {
            "status": "success",
            "mode": "PAPER",
            "orderId": _make_id("paper_order"),
            "symbol": symbol,
            "side": side,
            "quantity": qty,
            "trade_id": trade_id,
            "placed_at": _now().isoformat(),
        }

    _ensure_dhan_orders()
    if dhan_orders is None:
        if ORDER_FALLBACK_TO_PAPER:
            return _paper_fallback_order(symbol, qty, side, trade_id, "sandbox client unavailable")
        return {"status": "error", "message": "sandbox client unavailable"}

    try:
        resp = dhan_orders.place_order(
            security_id=_resolve_security_id(symbol),
            exchange_segment="NSE_EQ",
            transaction_type="BUY" if side == "BUY" else "SELL",
            quantity=qty,
            order_type="MARKET",
            product_type="INTRA",
            price=0,
        )
        status = str(resp.get("status", "")).lower() if isinstance(resp, dict) else ""
        if status in {"error", "failure", "rejected"} and ORDER_FALLBACK_TO_PAPER:
            return _paper_fallback_order(symbol, qty, side, trade_id, json.dumps(_serialize_value(resp)))
        return resp
    except Exception as exc:
        log.error("Order placement failed for %s: %s", symbol, exc)
        if ORDER_FALLBACK_TO_PAPER:
            return _paper_fallback_order(symbol, qty, side, trade_id, str(exc))
        return {"status": "error", "message": str(exc)}


def _prune_pool(pool: dict[str, list[datetime]], cutoff: datetime) -> dict[str, int]:
    counts = {}
    for symbol in list(pool.keys()):
        original = pool[symbol]
        valid = [stamp for stamp in original if stamp >= cutoff]
        if valid:
            pool[symbol] = valid
            counts[symbol] = len(valid)
            if len(valid) != len(original):
                _mark_state_dirty()
        else:
            pool.pop(symbol, None)
            _mark_state_dirty()
    return counts


def _prune_all_pools(now: datetime | None = None):
    cutoff = (now or _now()) - timedelta(minutes=ROLLING_WINDOW_MINS)
    with state_lock:
        _prune_pool(pool_1m, cutoff)
        _prune_pool(pool_3m, cutoff)
        _prune_pool(pool_5m, cutoff)
        _prune_pool(pool_buy, cutoff)
    return cutoff


def _rank_with_volume(counts: dict[str, int], symbols: list[str], limit: int) -> list[str]:
    if not symbols:
        return []
    _get_bulk_volumes(symbols)
    return sorted(symbols, key=lambda symbol: (-counts.get(symbol, 0), -volumes.get(symbol, 0), symbol))[:limit]


def _rebuild_master() -> list[str]:
    global master

    now = _now()
    cutoff = now - timedelta(minutes=ROLLING_WINDOW_MINS)
    with state_lock:
        counts_1m = _prune_pool(pool_1m, cutoff)
        counts_3m = _prune_pool(pool_3m, cutoff)
        counts_5m = _prune_pool(pool_5m, cutoff)

    top_1m = _rank_with_volume(counts_1m, list(counts_1m.keys()), TOP_1M)
    top_3m = _rank_with_volume(counts_3m, list(counts_3m.keys()), TOP_3M)
    combined = list(dict.fromkeys(top_1m + top_3m))
    final_master = _rank_with_volume(counts_5m, combined, MASTER_SIZE)

    with state_lock:
        master = final_master
        _save_master()
        _mark_state_dirty()

    depth_manager.ensure_symbols(combined[:50])
    log.info("MASTER UPDATED (%s): %s", len(final_master), final_master)
    _append_event(
        "baseline",
        "master_rebuild",
        {"top_1m": top_1m, "top_3m": top_3m, "combined": combined, "master": final_master},
    )
    return final_master


def _compute_runner_score(
    symbol: str,
    now: datetime | None = None,
    payload_time: datetime | None = None,
) -> tuple[int, dict]:
    now = now or _now()
    symbol = symbol.upper()
    with state_lock:
        cutoff = now - timedelta(minutes=ROLLING_WINDOW_MINS)
        counts_1m = len([stamp for stamp in pool_1m.get(symbol, []) if stamp >= cutoff])
        counts_3m = len([stamp for stamp in pool_3m.get(symbol, []) if stamp >= cutoff])
        counts_5m = len([stamp for stamp in pool_5m.get(symbol, []) if stamp >= cutoff])
        buy_count = len([stamp for stamp in pool_buy.get(symbol, []) if stamp >= cutoff])

    quote = _get_quote_snapshot(symbol) or {}
    ltp = float(quote.get("last_price", 0) or 0)
    day_open = float((quote.get("ohlc") or {}).get("open", 0) or 0)
    day_gain = ((ltp - day_open) / day_open) if day_open > 0 and ltp > 0 else 0.0

    if FINAL_1M_STRATEGY_ENABLED:
        candles = _get_1min_candles(symbol, n=45) or []
        signal_cutoff = _alert_signal_cutoff(now, payload_time=payload_time)
        score_candles = _candles_until(candles, signal_cutoff) or candles
        score_candle = score_candles[-1] if score_candles else None
        score_price = float(score_candle["close"]) if score_candle else ltp
        if ltp <= 0 and score_candle:
            ltp = score_price
        if day_open <= 0 and candles:
            day_open = float(candles[0]["open"])
        day_gain = ((score_price - day_open) / day_open) if day_open > 0 and score_price > 0 else day_gain

        rel_vol = 0.0
        if score_candles:
            recent_10_vol = sum(int(candle.get("volume", 0) or 0) for candle in score_candles[-10:])
            cumulative_vol = sum(int(candle.get("volume", 0) or 0) for candle in score_candles)
            elapsed_mins = max(
                1,
                int((signal_cutoff - signal_cutoff.replace(hour=9, minute=15, second=0, microsecond=0)).total_seconds() // 60) + 1,
            )
            expected_10_vol = (cumulative_vol / elapsed_mins) * min(10, elapsed_mins) if cumulative_vol > 0 else 0
            rel_vol = (recent_10_vol / expected_10_vol) if expected_10_vol > 0 else 0.0

        score = min(45, buy_count * 18) + min(40, counts_1m * 10)
        if day_gain >= 0.02:
            score += 15
        if day_gain >= 0.04:
            score += 10
        if rel_vol >= 1.5:
            score += 10
        if rel_vol >= 2.5:
            score += 10
        if day_gain > 0.09:
            score -= 15
        if score_candle:
            if float(score_candle["close"]) < float(score_candle["open"]) and day_gain < 0.04:
                score -= 8

        score_candle_time = _parse_candle_time(score_candle.get("time")) if score_candle else None
        features = {
            "runner_score": int(score),
            "score_model": "FINAL_BUY_1M",
            "counts_1m": counts_1m,
            "counts_3m": counts_3m,
            "counts_5m": counts_5m,
            "final_buy_count": buy_count,
            "day_gain_pct": round(day_gain * 100, 2),
            "rel_vol_10m": round(rel_vol, 2),
            "ltp": round(ltp, 2) if ltp else None,
            "score_candle_time": score_candle_time.isoformat() if score_candle_time else None,
            "score_signal_cutoff": signal_cutoff.isoformat(),
            "score_signal_source": "PAYLOAD_SIGNAL_TIME" if payload_time is not None else "ALERT_PREVIOUS_COMPLETED_1M",
            "payload_time": payload_time.isoformat() if payload_time else None,
        }
        return int(score), features

    score = 0
    score += min(30, counts_5m * 12)
    score += min(25, counts_3m * 6)
    score += min(20, counts_1m * 5)
    score += min(20, buy_count * 8)
    if day_gain >= 0.02:
        score += 15
    if day_gain >= 0.04:
        score += 10

    candles = []
    rel_vol = 0.0
    above_vwap = None
    # Historical candles are rate-limited, so hydrate only plausible runner candidates.
    if score >= 55:
        candles = _get_1min_candles(symbol, n=40) or []
    if ltp <= 0 and candles:
        ltp = float(candles[-1]["close"])
    if day_open <= 0 and candles:
        day_open = float(candles[0]["open"])
    if candles:
        day_gain = ((ltp - day_open) / day_open) if day_open > 0 and ltp > 0 else day_gain
        recent_10_vol = sum(int(candle.get("volume", 0) or 0) for candle in candles[-10:])
        cumulative_vol = sum(int(candle.get("volume", 0) or 0) for candle in candles)
        elapsed_mins = max(1, int((now - now.replace(hour=9, minute=15, second=0, microsecond=0)).total_seconds() // 60) + 1)
        expected_10_vol = (cumulative_vol / elapsed_mins) * min(10, elapsed_mins) if cumulative_vol > 0 else 0
        rel_vol = (recent_10_vol / expected_10_vol) if expected_10_vol > 0 else 0.0

        pv_total = 0.0
        vol_total = 0
        for candle in candles:
            volume = int(candle.get("volume", 0) or 0)
            typical = (float(candle["high"]) + float(candle["low"]) + float(candle["close"])) / 3
            pv_total += typical * volume
            vol_total += volume
        if rel_vol >= 1.5:
            score += 10
        if rel_vol >= 2.5:
            score += 10
        last = candles[-1]
        if float(last["close"]) < float(last["open"]) and day_gain < 0.04:
            score -= 8
    if day_gain > 0.09:
        score -= 15

    features = {
        "runner_score": int(score),
        "counts_1m": counts_1m,
        "counts_3m": counts_3m,
        "counts_5m": counts_5m,
        "final_buy_count": buy_count,
        "day_gain_pct": round(day_gain * 100, 2),
        "rel_vol_10m": round(rel_vol, 2),
        "above_vwap": above_vwap,
        "ltp": round(ltp, 2) if ltp else None,
    }
    return int(score), features


def _runner_mode_for_score(score: int) -> str:
    if STRATEGY_MODE == "SUPREME_RUNNER_V2" and score >= RUNNER_MODE_SCORE:
        return "RUNNER"
    return "BASE"


def _is_market_warmup(now: datetime | None = None) -> bool:
    current = (now or _now()).time()
    return dt_time(9, 15) <= current < WARMUP_END


def _is_eod(now: datetime | None = None) -> bool:
    return (now or _now()).time() >= EOD_EXIT_TIME


def _build_depth_context(symbol: str, signal_price: float) -> dict:
    depth_now, depth_5s_ago, rolling = depth_manager.get(symbol)
    quote = _get_quote_snapshot(symbol)
    turnover_cr = _get_daily_turnover_cr(symbol, quote)
    recent_30min_volume = _get_recent_30min_volume(symbol)
    velocity = 0.0
    gate_pass = False
    gate_score = 0
    gate_reason = "DEPTH_DISABLED"
    rank_score = None
    spread_pct = None

    if DEPTH_SHADOW_ENABLED and depth_now and depth_now.get("buy") and depth_now.get("sell"):
        best_bid = depth_now["buy"][0]["price"]
        best_ask = depth_now["sell"][0]["price"]
        if best_bid > 0:
            spread_pct = (best_ask - best_bid) / best_bid
        gate_pass, gate_score, gate_reason = entry_gate(
            symbol=symbol,
            signal_price=signal_price,
            depth_now=depth_now,
            depth_5s_ago=depth_5s_ago,
            daily_turnover_cr=turnover_cr,
            recent_30min_volume=recent_30min_volume,
        )
        rank_score = rank_signal(
            symbol=symbol,
            signal_price=signal_price,
            gate_score=gate_score,
            depth_now=depth_now,
            depth_buffer_60s=rolling,
            recent_30min_volume=recent_30min_volume,
        )
        from depth_shadow import compute_book_velocity

        velocity = compute_book_velocity(rolling)
    elif DEPTH_SHADOW_ENABLED:
        gate_reason = "NO_DEPTH_DATA"

    return {
        "depth_now": depth_now,
        "depth_5s_ago": depth_5s_ago,
        "rolling_60s_len": len(rolling),
        "quote": quote,
        "turnover_cr": turnover_cr,
        "recent_30min_volume": recent_30min_volume,
        "gate_pass": gate_pass,
        "gate_score": gate_score,
        "gate_reason": gate_reason,
        "rank_score": rank_score,
        "velocity": velocity,
        "spread_pct": spread_pct,
    }


def _queue_signal(symbol: str, runner_ctx: dict | None = None, payload_time: datetime | None = None) -> bool:
    queued_at = _now()
    candles = _get_1min_candles(symbol, n=10)
    if not candles:
        log.info("QUEUE SKIPPED %s | reason=NO_1M_DATA", symbol)
        return False

    signal_candle, signal_time, signal_cutoff, signal_source = _select_alert_signal_candle(
        candles,
        queued_at,
        payload_time=payload_time,
    )
    if not signal_candle:
        log.info("QUEUE SKIPPED %s | reason=NO_SIGNAL_CANDLE", symbol)
        return False
    signal_time = signal_time or signal_cutoff

    with state_lock:
        pending[symbol] = {
            "queued_at": queued_at,
            "expires_at": queued_at + timedelta(minutes=SMART_ENTRY_MINS),
            "signal_high": float(signal_candle["high"]),
            "signal_close": float(signal_candle["close"]),
            "signal_candle_time": signal_time,
            "last_candle_check": signal_time,
            "signal_payload": {
                "high": float(signal_candle["high"]),
                "close": float(signal_candle["close"]),
                "time": signal_time.isoformat(),
                "cutoff": signal_cutoff.isoformat(),
                "source": signal_source,
            },
            "depth_signal_ctx": {"gate_reason": "NOT_EVALUATED_AT_QUEUE"},
            "runner_ctx": runner_ctx or {},
            "mode": (runner_ctx or {}).get("mode", "BASE"),
            "runner_score": int((runner_ctx or {}).get("runner_score", 0) or 0),
        }
        _mark_state_dirty()

    _append_event(
        "baseline",
        "pending_queued",
        {
            "symbol": symbol,
            "signal_high": float(signal_candle["high"]),
            "signal_close": float(signal_candle["close"]),
            "signal_candle_time": signal_time.isoformat(),
            "signal_cutoff": signal_cutoff.isoformat(),
            "signal_source": signal_source,
            "payload_time": payload_time.isoformat() if payload_time else None,
            "queued_at": queued_at.isoformat(),
            "expires_at": (queued_at + timedelta(minutes=SMART_ENTRY_MINS)).isoformat(),
            "runner_ctx": _serialize_value(runner_ctx or {}),
        },
    )
    if DEPTH_SHADOW_ENABLED:
        depth_manager.ensure_symbols([symbol])
        depth_now, depth_5s_ago, rolling = depth_manager.get(symbol)
        _append_event(
            "shadow",
            "signal_observed",
            {
                "symbol": symbol,
                "signal_high": float(signal_candle["high"]),
                "signal_close": float(signal_candle["close"]),
                "signal_candle_time": signal_time.isoformat(),
                "signal_source": signal_source,
                "payload_time": payload_time.isoformat() if payload_time else None,
                "queued_at": queued_at.isoformat(),
                "depth": _serialize_value(
                    {
                        "depth_now": depth_now,
                        "depth_5s_ago": depth_5s_ago,
                        "rolling_60s_len": len(rolling),
                        "gate_reason": "QUEUE_DEPTH_SNAPSHOT_ONLY",
                    }
                ),
            },
        )
    return True


def _extract_healthy_dip(symbol: str, meta: dict) -> dict | None:
    candles = _get_1min_candles(symbol, n=6)
    if not candles:
        return None

    last_seen = meta.get("last_candle_check") or meta.get("signal_candle_time")
    newest_seen = last_seen
    for candle in candles:
        candle_time = _parse_candle_time(candle.get("time"))
        if candle_time is None:
            continue

        if newest_seen is None or candle_time > newest_seen:
            newest_seen = candle_time

        if last_seen and candle_time <= last_seen:
            continue
        if candle_time > meta["expires_at"]:
            continue

        open_price = float(candle["open"])
        close_price = float(candle["close"])
        if open_price <= 0 or close_price >= open_price:
            continue

        dip_pct = (open_price - close_price) / open_price
        if dip_pct <= MAX_RED_CANDLE_PCT:
            with state_lock:
                if symbol in pending:
                    pending[symbol]["last_candle_check"] = newest_seen
                    _mark_state_dirty()
            return {"entry_price": close_price, "dip_pct": dip_pct, "candle_time": candle_time}

    with state_lock:
        if symbol in pending:
            pending[symbol]["last_candle_check"] = newest_seen
            _mark_state_dirty()
    return None


def _extract_final_1m_entry(symbol: str, meta: dict) -> dict | None:
    candles = _get_1min_candles(symbol, n=20)
    if not candles:
        return None

    last_seen = meta.get("last_candle_check") or meta.get("signal_candle_time")
    newest_seen = last_seen
    for idx, candle in enumerate(candles):
        candle_time = _parse_candle_time(candle.get("time"))
        if candle_time is None:
            continue

        if newest_seen is None or candle_time > newest_seen:
            newest_seen = candle_time

        if last_seen and candle_time <= last_seen:
            continue
        if candle_time > meta["expires_at"]:
            continue

        open_price = float(candle.get("open", 0) or 0)
        high_price = float(candle.get("high", 0) or 0)
        close_price = float(candle.get("close", 0) or 0)
        if open_price <= 0 or high_price <= meta["signal_high"]:
            continue
        if close_price < open_price:
            continue

        close_position = _candle_close_position(candle)
        if close_position is None or close_position < ENTRY_CLOSE_POSITION_MIN:
            continue

        curr10 = _volume_curr10(candles, idx)
        if curr10 is None or curr10 < ENTRY_VOLUME_CURR10_MIN:
            continue

        with state_lock:
            if symbol in pending:
                pending[symbol]["last_candle_check"] = newest_seen
                _mark_state_dirty()

        ltp = _get_ltp(symbol)
        entry_price = float(ltp) if ltp and ltp > 0 else close_price
        return {
            "entry_price": entry_price,
            "trigger": "BREAKOUT_GREEN_CP75_VOL",
            "candle_time": candle_time,
            "close_position": close_position,
            "volume_curr10": curr10,
        }

    with state_lock:
        if symbol in pending:
            pending[symbol]["last_candle_check"] = newest_seen
            _mark_state_dirty()
    return None


def _open_shadow_position(symbol: str, price: float, qty: int, trigger: str, baseline_trade_id: str):
    depth_ctx = _build_depth_context(symbol, price)
    payload = {
        "symbol": symbol,
        "baseline_trade_id": baseline_trade_id,
        "entry_price": round(price, 2),
        "trigger": trigger,
        "depth": _serialize_value(depth_ctx),
    }

    if not depth_ctx["gate_pass"]:
        _append_event("shadow", "entry_rejected", payload | {"reason": depth_ctx["gate_reason"]})
        return

    shadow_trade_id = _make_id("shadow_trade")
    entry_time = _now()
    with state_lock:
        shadow_positions[symbol] = {
            "trade_id": shadow_trade_id,
            "baseline_trade_id": baseline_trade_id,
            "entry": price,
            "entry_time": entry_time,
            "peak": price,
            "hard_sl": round(price * (1 - SL_PCT), 2),
            "sl": round(price * (1 - SL_PCT), 2),
            "tsl_on": False,
            "qty": qty,
            "trigger": trigger,
            "recent_30min_volume": depth_ctx["recent_30min_volume"],
            "gate_score": depth_ctx["gate_score"],
            "gate_reason": depth_ctx["gate_reason"],
            "rank_score": depth_ctx["rank_score"],
            "neg_velocity_since": None,
            "wall_timers": {},
            "last_snapshot_log": 0.0,
        }
        _mark_state_dirty()

    _append_event("shadow", "entry_opened", payload | {"shadow_trade_id": shadow_trade_id})


def _execute_entry(symbol: str, price: float, trigger: str) -> bool:
    with state_lock:
        pending_meta = dict(pending.get(symbol) or {})
        if symbol in positions:
            pending.pop(symbol, None)
            _mark_state_dirty()
            return False
        if len(positions) >= MAX_SLOTS:
            return False

    trade_id = _make_id("base_trade")
    qty = max(1, int(SLOT_CAPITAL / price))
    order_resp = _place_order(symbol, qty, "BUY", trade_id)
    status = str(order_resp.get("status", "")).lower() if isinstance(order_resp, dict) else ""
    accepted = status not in {"error", "failure", "rejected"}
    if not accepted:
        with state_lock:
            pending.pop(symbol, None)
            _mark_state_dirty()
        log.error("ENTRY FAILED %s | response=%s", symbol, order_resp)
        _append_event(
            "baseline",
            "entry_failed",
            {
                "symbol": symbol,
                "trade_id": trade_id,
                "entry_price": round(price, 2),
                "qty": qty,
                "trigger": trigger,
                "order_response": _serialize_value(order_resp),
            },
        )
        return False

    entry_time = _now()
    with state_lock:
        pending.pop(symbol, None)
        positions[symbol] = {
            "trade_id": trade_id,
            "entry": price,
            "entry_time": entry_time,
            "peak": price,
            "hard_sl": round(price * (1 - SL_PCT), 2),
            "sl": round(price * (1 - SL_PCT), 2),
            "tsl_on": False,
            "qty": qty,
            "trigger": trigger,
            "mode": pending_meta.get("mode", "BASE"),
            "runner_score": int(pending_meta.get("runner_score", 0) or 0),
            "runner_ctx": pending_meta.get("runner_ctx", {}),
            "order": order_resp,
        }
        _mark_state_dirty()

    _append_event(
        "baseline",
        "entry_opened",
        {
            "symbol": symbol,
            "trade_id": trade_id,
            "entry_price": round(price, 2),
            "qty": qty,
            "trigger": trigger,
            "mode": TRADING_MODE,
            "strategy_mode": STRATEGY_MODE,
            "position_mode": pending_meta.get("mode", "BASE"),
            "runner_score": int(pending_meta.get("runner_score", 0) or 0),
        },
    )
    _open_shadow_position(symbol, price, qty, trigger, trade_id)
    return True


def _close_shadow_position(symbol: str, pos: dict, reason: str, exit_price: float, depth_reason: str | None = None):
    pnl_rs = ((exit_price - pos["entry"]) / pos["entry"]) * SLOT_CAPITAL
    trade = {
        "trade_id": pos["trade_id"],
        "baseline_trade_id": pos["baseline_trade_id"],
        "symbol": symbol,
        "entry_p": round(pos["entry"], 2),
        "exit_p": round(exit_price, 2),
        "reason": reason,
        "depth_reason": depth_reason,
        "gate_score": pos.get("gate_score"),
        "rank_score": pos.get("rank_score"),
        "pnl_rs": round(pnl_rs, 2),
        "entry_time": pos["entry_time"].isoformat(),
        "exit_time": _now().isoformat(),
    }
    with state_lock:
        shadow_positions.pop(symbol, None)
        shadow_trades.append(trade)
        _mark_state_dirty()
    _append_event("shadow", "exit_closed", trade)


def _exit(symbol: str, pos: dict, reason: str, exit_price: float):
    order_resp = _place_order(symbol, pos["qty"], "SELL", pos["trade_id"])
    pnl_rs = ((exit_price - pos["entry"]) / pos["entry"]) * SLOT_CAPITAL
    trade = {
        "trade_id": pos["trade_id"],
        "symbol": symbol,
        "entry_p": round(pos["entry"], 2),
        "exit_p": round(exit_price, 2),
        "reason": reason,
        "trigger": pos.get("trigger"),
        "pnl_rs": round(pnl_rs, 2),
        "entry_time": pos["entry_time"].isoformat(),
        "exit_time": _now().isoformat(),
        "order": order_resp,
        "supertrend_ctx": pos.get("supertrend_ctx"),
    }
    with state_lock:
        positions.pop(symbol, None)
        trades_today.append(trade)
        _mark_state_dirty()
    _append_event("baseline", "exit_closed", trade)


def _evaluate_pending(symbol: str):
    with state_lock:
        meta = pending.get(symbol)
        current_positions = len(positions)

    if not meta:
        return

    now = _now()
    if now >= meta["expires_at"]:
        with state_lock:
            pending.pop(symbol, None)
            _mark_state_dirty()
        _append_event("baseline", "pending_expired", {"symbol": symbol})
        return

    if current_positions >= MAX_SLOTS:
        return

    if FINAL_1M_STRATEGY_ENABLED:
        entry = _extract_final_1m_entry(symbol, meta)
        if entry:
            _append_event(
                "baseline",
                "final_1m_entry_confirmed",
                {
                    "symbol": symbol,
                    "candle_time": entry["candle_time"].isoformat(),
                    "close_position": round(entry["close_position"], 3),
                    "volume_curr10": round(entry["volume_curr10"], 3),
                    "entry_price": round(entry["entry_price"], 2),
                },
            )
            _execute_entry(symbol, entry["entry_price"], entry["trigger"])
        return

    ltp = _get_ltp(symbol)
    if ltp is not None and ltp > meta["signal_high"]:
        _execute_entry(symbol, ltp, "BREAKOUT")
        return

    healthy_dip = _extract_healthy_dip(symbol, meta)
    if healthy_dip:
        _append_event(
            "baseline",
            "healthy_dip_detected",
            {
                "symbol": symbol,
                "dip_pct": round(healthy_dip["dip_pct"] * 100, 3),
                "candle_time": healthy_dip["candle_time"].isoformat(),
            },
        )
        _execute_entry(symbol, healthy_dip["entry_price"], "HEALTHY_DIP")


def _evaluate_positions():
    with state_lock:
        snapshot = [(symbol, dict(pos)) for symbol, pos in positions.items()]

    now = _now()
    eod = _is_eod(now)
    for symbol, pos in snapshot:
        ltp = _get_ltp(symbol)
        if ltp is None:
            continue

        position_mode = pos.get("mode", "BASE")
        is_runner = position_mode == "RUNNER"
        tsl_trigger = RUNNER_TSL_TRIGGER_PCT if is_runner else TSL_TRIGGER_PCT
        tsl_trail = RUNNER_TSL_TRAIL_PCT if is_runner else TSL_TRAIL_PCT

        if ltp > pos["peak"]:
            pos["peak"] = ltp
        if not pos["tsl_on"] and ltp >= pos["entry"] * (1 + tsl_trigger):
            pos["tsl_on"] = True
        if pos["tsl_on"]:
            trailing_floor = pos["peak"] * (1 - tsl_trail)
            if trailing_floor > pos["sl"]:
                pos["sl"] = round(trailing_floor, 2)

        with state_lock:
            live_pos = positions.get(symbol)
            if live_pos is None:
                continue
            live_pos["peak"] = pos["peak"]
            live_pos["tsl_on"] = pos["tsl_on"]
            live_pos["sl"] = pos["sl"]
            _mark_state_dirty()
            pos = dict(live_pos)

        reason = None
        if eod:
            reason = "EOD_EXIT"
        elif ltp <= pos["hard_sl"]:
            reason = "SL_HIT"
        elif ltp <= pos["sl"] and pos["tsl_on"]:
            reason = "TSL_HIT"
        elif SUPERTREND_EXIT_ENABLED and pos.get("tsl_on"):
            bearish, st_ctx = _is_supertrend_bearish(symbol)
            if bearish:
                reason = "ST_AFTER_TSL_EXIT"
                pos["supertrend_ctx"] = st_ctx
        elif is_runner and (now - pos["entry_time"]).total_seconds() >= RUNNER_STALL_MINS * 60 and pos["peak"] < pos["entry"] * (1 + RUNNER_STALL_PEAK_PCT):
            reason = "RUNNER_STALL_EXIT"
        elif not is_runner and (now - pos["entry_time"]).total_seconds() >= TIME_EXIT_MINS * 60:
            reason = "TIME_EXIT"

        if reason:
            _exit(symbol, pos, reason, ltp)


def _evaluate_shadow_positions():
    with state_lock:
        snapshot = [(symbol, dict(pos)) for symbol, pos in shadow_positions.items()]

    now = _now()
    eod = _is_eod(now)
    for symbol, pos in snapshot:
        ltp = _get_ltp(symbol)
        depth_now, _, rolling = depth_manager.get(symbol)
        action = "CONTINUE"
        action_reason = "NO_DEPTH"

        if depth_now:
            position_ctx = {
                "entry_price": pos["entry"],
                "peak_price": pos["peak"],
                "symbol": symbol,
                "neg_velocity_since": pos.get("neg_velocity_since"),
            }
            action, action_reason = watchdog_tick(
                position=position_ctx,
                depth_now=depth_now,
                depth_buffer_60s=rolling,
                wall_timers=pos.get("wall_timers", {}),
                recent_30min_volume=pos.get("recent_30min_volume", 0),
            )
            pos["neg_velocity_since"] = position_ctx.get("neg_velocity_since")

        if ltp is None:
            continue

        if ltp > pos["peak"]:
            pos["peak"] = ltp
        if not pos["tsl_on"] and ltp >= pos["entry"] * (1 + TSL_TRIGGER_PCT):
            pos["tsl_on"] = True
        if pos["tsl_on"]:
            trailing_floor = pos["peak"] * (1 - TSL_TRAIL_PCT)
            if trailing_floor > pos["sl"]:
                pos["sl"] = round(trailing_floor, 2)

        with state_lock:
            live_pos = shadow_positions.get(symbol)
            if live_pos is None:
                continue
            live_pos["peak"] = pos["peak"]
            live_pos["tsl_on"] = pos["tsl_on"]
            live_pos["sl"] = pos["sl"]
            live_pos["neg_velocity_since"] = pos.get("neg_velocity_since")
            live_pos["wall_timers"] = pos.get("wall_timers", {})
            _mark_state_dirty()
            pos = dict(live_pos)

        now_ts = time.time()
        if now_ts - pos.get("last_snapshot_log", 0.0) >= SHADOW_SNAPSHOT_LOG_SECS:
            _append_event(
                "shadow",
                "position_snapshot",
                {
                    "trade_id": pos["trade_id"],
                    "symbol": symbol,
                    "ltp": round(ltp, 2),
                    "peak": round(pos["peak"], 2),
                    "sl": round(pos["sl"], 2),
                    "tsl_on": pos["tsl_on"],
                    "depth_action": action,
                    "depth_reason": action_reason,
                    "buffer_len": len(rolling),
                },
            )
            with state_lock:
                if symbol in shadow_positions:
                    shadow_positions[symbol]["last_snapshot_log"] = now_ts
                    _mark_state_dirty()

        if action == "EXIT":
            _close_shadow_position(symbol, pos, "DEPTH_EXIT", ltp, action_reason)
            continue

        reason = None
        if eod:
            reason = "EOD_EXIT"
        elif ltp <= pos["hard_sl"]:
            reason = "SL_HIT"
        elif action != "HOLD" and ltp <= pos["sl"] and pos["tsl_on"]:
            reason = "TSL_HIT"
        elif action != "HOLD" and SUPERTREND_EXIT_ENABLED and pos.get("tsl_on"):
            bearish, st_ctx = _is_supertrend_bearish(symbol)
            if bearish:
                reason = "ST_AFTER_TSL_EXIT"
                action_reason = f"SUPERTREND_{st_ctx.get('period')}_{st_ctx.get('multiplier')}"
        elif action != "HOLD" and (now - pos["entry_time"]).total_seconds() >= TIME_EXIT_MINS * 60:
            reason = "TIME_EXIT"

        if reason:
            _close_shadow_position(symbol, pos, reason, ltp, action_reason)


def _cancel_pending_eod():
    if not _is_eod():
        return

    with state_lock:
        if not pending:
            return
        cancelled = list(pending.keys())
        pending.clear()
        _mark_state_dirty()
    _append_event("baseline", "pending_cleared_eod", {"symbols": cancelled})


def watchdog():
    while True:
        try:
            _prune_all_pools()
            _cancel_pending_eod()
            with state_lock:
                pending_symbols = sorted(
                    pending.keys(),
                    key=lambda symbol: int((pending.get(symbol) or {}).get("runner_score", 0) or 0),
                    reverse=True,
                )
            for symbol in pending_symbols:
                _evaluate_pending(symbol)
            _evaluate_positions()
            _evaluate_shadow_positions()
            _save_runtime_state()
        except Exception as exc:
            log.error("Watchdog error: %s", exc, exc_info=True)
            _append_event("shadow", "watchdog_error", {"message": str(exc)})
            _save_runtime_state(force=True)
        time.sleep(1)


def _bootstrap_runtime():
    global watchdog_started
    _init_event_db()
    _load_master()
    _load_runtime_state()
    _load_recent_shadow_events()
    _prune_all_pools()
    depth_manager.start()
    depth_manager.ensure_symbols(list(set(master) | set(positions.keys()) | set(shadow_positions.keys()) | set(pending.keys())))

    if watchdog_started:
        return

    threading.Thread(target=watchdog, daemon=True, name="watchdog").start()
    watchdog_started = True
    _save_runtime_state(force=True)
    log.info("Watchdog started. Mode=%s depth_shadow=%s", TRADING_MODE, DEPTH_SHADOW_ENABLED)


atexit.register(lambda: _save_runtime_state(force=True))


def _ingest_symbols(pool: dict[str, list[datetime]], label: str):
    symbols = _parse_symbols_from_request()
    now = _now()
    with state_lock:
        for symbol in symbols:
            pool.setdefault(symbol, []).append(now)
        _mark_state_dirty()
    _prune_all_pools(now)
    _append_event("baseline", "scanner_ingest", {"label": label, "symbols": symbols})
    return symbols


@app.get("/")
def dashboard():
    with state_lock:
        baseline_positions = _serialize_value(positions)
        baseline_trades = _serialize_value(trades_today[-30:])
        shadow_open = _serialize_value(shadow_positions)
        shadow_closed = _serialize_value(shadow_trades[-30:])
        pending_view = _serialize_value(pending)
        master_view = list(master)
        pools = {
            "1m": sum(len(stamps) for stamps in pool_1m.values()),
            "3m": sum(len(stamps) for stamps in pool_3m.values()),
            "5m": sum(len(stamps) for stamps in pool_5m.values()),
            "final_buy": sum(len(stamps) for stamps in pool_buy.values()),
        }

    return jsonify(
        {
            "status": "ONLINE",
            "mode": TRADING_MODE,
            "strategy_mode": STRATEGY_MODE,
            "runner_config": {
                "override_score": RUNNER_OVERRIDE_SCORE,
                "runner_mode_score": RUNNER_MODE_SCORE,
                "stall_mins": RUNNER_STALL_MINS,
                "stall_peak_pct": RUNNER_STALL_PEAK_PCT,
                "tsl_trigger_pct": RUNNER_TSL_TRIGGER_PCT,
                "tsl_trail_pct": RUNNER_TSL_TRAIL_PCT,
            },
            "final_1m_config": {
                "enabled": FINAL_1M_STRATEGY_ENABLED,
                "trading_start_time": WARMUP_END.strftime("%H:%M"),
                "order_fallback_to_paper": ORDER_FALLBACK_TO_PAPER,
                "entry_close_position_min": ENTRY_CLOSE_POSITION_MIN,
                "entry_volume_curr10_min": ENTRY_VOLUME_CURR10_MIN,
                "sl_pct": SL_PCT,
                "tsl_trigger_pct": TSL_TRIGGER_PCT,
                "tsl_trail_pct": TSL_TRAIL_PCT,
                "time_exit_mins": TIME_EXIT_MINS,
                "supertrend_exit_enabled": SUPERTREND_EXIT_ENABLED,
                "supertrend_atr_period": SUPERTREND_ATR_PERIOD,
                "supertrend_multiplier": SUPERTREND_MULTIPLIER,
            },
            "depth_shadow_enabled": DEPTH_SHADOW_ENABLED,
            "token_health": {
                "main_data": _jwt_meta(MAIN_ACCESS_TOKEN, MAIN_CLIENT_ID, "MAIN_ACCESS_TOKEN"),
                "sandbox_orders": _jwt_meta(SANDBOX_ACCESS_TOKEN, SANDBOX_CLIENT_ID, "SANDBOX_ACCESS_TOKEN"),
            },
            "now_ist": _now().isoformat(),
            "master": master_view,
            "pending": pending_view,
            "positions": baseline_positions,
            "shadow_positions": shadow_open,
            "pools": pools,
            "baseline": {
                "open_count": len(baseline_positions),
                "trade_count": len(trades_today),
                "total_pnl": round(sum(trade["pnl_rs"] for trade in trades_today), 2),
                "recent_trades": baseline_trades,
            },
            "shadow": {
                "open_count": len(shadow_open),
                "trade_count": len(shadow_trades),
                "total_pnl": round(sum(trade["pnl_rs"] for trade in shadow_trades), 2),
                "recent_trades": shadow_closed,
                "recent_events": list(shadow_recent_events)[-50:],
            },
            "depth_feed": depth_manager.status(),
            "depth_buffer": depth_manager.buffer.stats(),
            "log_dir": LOG_DIR,
            "state_file": STATE_FILE,
            "event_db_file": EVENT_DB_FILE,
            "railway_volume_mount_path": RAILWAY_VOLUME_ROOT,
        }
    )


@app.get("/shadow")
def shadow_dashboard():
    with state_lock:
        payload = {
            "open_positions": _serialize_value(shadow_positions),
            "closed_trades": _serialize_value(shadow_trades[-100:]),
            "recent_events": list(shadow_recent_events),
            "depth_feed": depth_manager.status(),
        }
    return jsonify(payload)


@app.get("/admin/trades")
def admin_trades():
    day = request.args.get("day")
    with state_lock:
        baseline_all = _filter_trades_by_day(_serialize_value(trades_today), day)
        shadow_all = _filter_trades_by_day(_serialize_value(shadow_trades), day)

    return jsonify(
        {
            "day": day,
            "baseline": {
                "trade_count": len(baseline_all),
                "total_pnl": round(sum(float(trade.get("pnl_rs", 0) or 0) for trade in baseline_all), 2),
                "trades": baseline_all,
            },
            "shadow": {
                "trade_count": len(shadow_all),
                "total_pnl": round(sum(float(trade.get("pnl_rs", 0) or 0) for trade in shadow_all), 2),
                "trades": shadow_all,
            },
        }
    )


@app.get("/admin/events")
def admin_events():
    stream = request.args.get("stream")
    event_type = request.args.get("event")
    day = request.args.get("day")
    limit = request.args.get("limit", default=200, type=int)
    rows = _query_event_rows(stream=stream, event_type=event_type, day=day, limit=limit)
    return jsonify(
        {
            "stream": stream,
            "event": event_type,
            "day": day,
            "count": len(rows),
            "events": rows,
        }
    )


@app.post("/admin/reset")
def admin_reset():
    if not ADMIN_RESET_TOKEN or request.args.get("token") != ADMIN_RESET_TOKEN:
        return jsonify({"status": "forbidden", "message": "admin reset is disabled or token is invalid"}), 403
    reason = request.args.get("reason") or "manual_reset"
    return jsonify({"status": "ok", **_reset_runtime_state(reason=reason)})


@app.post("/webhook/1min")
def wh1():
    _ingest_symbols(pool_1m, "WH-1m")
    return jsonify(status="ok")


@app.post("/webhook/3min")
def wh3():
    _ingest_symbols(pool_3m, "WH-3m")
    return jsonify(status="ok")


@app.post("/webhook/5min")
def wh5():
    _ingest_symbols(pool_5m, "WH-5m")
    _rebuild_master()
    return jsonify(status="ok")


@app.post("/webhook/final_buy")
def wh_buy():
    now = _now()
    symbols = _parse_symbols_from_request()
    payload_times = _request_signal_time_by_symbol()
    if _is_market_warmup(now):
        _append_event("baseline", "warmup_bypass", {"symbols": symbols})
        return jsonify(status="warmup", skipped=symbols)

    latest_master = list(master) if FINAL_1M_STRATEGY_ENABLED else _rebuild_master()
    queued = []
    skipped = []
    with state_lock:
        open_symbols = set(positions.keys())
        pending_symbols = set(pending.keys())
        for symbol in symbols:
            pool_buy.setdefault(symbol.upper(), []).append(now)
        _mark_state_dirty()

    for symbol in symbols:
        symbol = symbol.upper()
        payload_time = payload_times.get(symbol)
        runner_score, runner_features = _compute_runner_score(symbol, now, payload_time=payload_time)
        in_master = symbol in latest_master
        runner_allowed = STRATEGY_MODE == "SUPREME_RUNNER_V2" and runner_score >= RUNNER_OVERRIDE_SCORE
        final_1m_allowed = FINAL_1M_STRATEGY_ENABLED and runner_score >= RUNNER_OVERRIDE_SCORE
        runner_ctx = runner_features | {
            "in_master": in_master,
            "strategy_mode": STRATEGY_MODE,
            "mode": _runner_mode_for_score(runner_score),
            "score_floor": RUNNER_OVERRIDE_SCORE,
            "final_1m_strategy_enabled": FINAL_1M_STRATEGY_ENABLED,
        }
        reason = None
        if symbol in BLACKLIST:
            reason = "BLACKLISTED"
        elif FINAL_1M_STRATEGY_ENABLED and not final_1m_allowed:
            reason = "SCORE_BELOW_FLOOR"
        elif not FINAL_1M_STRATEGY_ENABLED and not in_master and not runner_allowed:
            reason = "NOT_IN_MASTER"
        elif symbol in open_symbols:
            reason = "ALREADY_OPEN"
        elif symbol in pending_symbols:
            reason = "ALREADY_PENDING"
        elif _is_eod(now):
            reason = "EOD_BLOCK"

        if reason:
            skipped.append({"symbol": symbol, "reason": reason})
            _append_event("baseline", "buy_skipped", {"symbol": symbol, "reason": reason, "runner_ctx": runner_ctx})
            continue

        if _queue_signal(symbol, runner_ctx=runner_ctx, payload_time=payload_time):
            queued.append(symbol)
            if FINAL_1M_STRATEGY_ENABLED:
                _append_event("baseline", "final_1m_score_queued", {"symbol": symbol, "runner_ctx": runner_ctx})
            elif not in_master and runner_allowed:
                _append_event("baseline", "runner_override_queued", {"symbol": symbol, "runner_ctx": runner_ctx})
        else:
            skipped.append({"symbol": symbol, "reason": "QUEUE_FAILED"})

    return jsonify(status="ok", queued=queued, skipped=skipped)


_bootstrap_runtime()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
