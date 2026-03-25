import json
import math
import os
import re
import sys
import time
import atexit
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

# =========================
# НАСТРОЙКИ
# =========================

PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
WALLET_ADDRESS = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip().lower()
SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))

CHAIN_ID = 137
CHECK_INTERVAL = int(os.getenv("POLYMARKET_CHECK_INTERVAL_SEC", "5"))
DRY_RUN = True

# Бот смотрит покупки на сегодня и завтра по UTC.
LOOKAHEAD_DAYS = 2
DATE_SHIFT_TOLERANCE_DAYS = int(os.getenv("POLYMARKET_DATE_SHIFT_TOLERANCE_DAYS", "2"))

# BUY-правила.
BUY_MIN_PRICE = 0.35
BUY_MAX_PRICE = 0.65
BUY_SIZE_USD = 1.00

# Выход из позиции.
TAKE_PROFIT_PCT = 0.30
MAX_DRAWDOWN = 0.20
ABSOLUTE_SELL_PRICE = 0.25

# Лимит новых покупок за один UTC-день.
MAX_NEW_BUYS_PER_DAY = 50

# Тренд-фильтры для входа (buy).
MOMENTUM_LOOKBACK_1H_SEC = 3600
MOMENTUM_LOOKBACK_6H_SEC = 21600
MAX_PULLBACK_FROM_6H_HIGH = 0.15

# Доп-фильтры качества входа.
MAX_SPREAD_CENTS = float(os.getenv("POLYMARKET_MAX_SPREAD_CENTS", "8"))
# Оставляем опциональный процентный фильтр пустым по умолчанию.
# Пример включения: POLYMARKET_MAX_SPREAD_PCT=0.12
MAX_SPREAD_PCT_RAW = os.getenv("POLYMARKET_MAX_SPREAD_PCT", "").strip()
MIN_MARKET_VOLUME_USD = float(os.getenv("POLYMARKET_MIN_MARKET_VOLUME_USD", "1000"))
MAX_MARKETS_PER_SCAN = int(os.getenv("POLYMARKET_MAX_MARKETS_PER_SCAN", "120"))
ENABLE_BUY_SCAN = os.getenv("POLYMARKET_ENABLE_BUY_SCAN", "1").strip().lower() not in {"0", "false", "no", "off"}
BUY_SCAN_MAX_DURATION_SEC = int(os.getenv("POLYMARKET_BUY_SCAN_MAX_DURATION_SEC", "20"))
SIMPLE_ENTRY_MODE = os.getenv("POLYMARKET_SIMPLE_ENTRY_MODE", "1").strip().lower() not in {"0", "false", "no", "off"}

MIN_POSITION_SIZE = float(os.getenv("POLYMARKET_MIN_POSITION_SIZE", "0.01"))
SELL_SIZE_BUFFER = 0.995
STATE_FILE = Path(os.getenv("POLYMARKET_STATE_FILE", "weather_combined_bot_state.json"))
LOG_FILE = os.getenv("POLYMARKET_LOG_FILE", "").strip()
DISABLE_FILE_LOG = os.getenv("POLYMARKET_DISABLE_FILE_LOG", "").strip().lower() in {"1", "true", "yes", "on"}

WEATHER_SLUG_PREFIX = "weather/temperature"
WEATHER_KEYWORDS = (
    "highest temperature",
    "highest temp",
)

DATA_API_URL = "https://data-api.polymarket.com/positions"
PROFILE_API_URL = "https://gamma-api.polymarket.com/public-profile"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_TAGS_URL = "https://gamma-api.polymarket.com/tags"
CLOB_API_URL = "https://clob.polymarket.com"
REQUEST_TIMEOUT = int(os.getenv("POLYMARKET_REQUEST_TIMEOUT_SEC", "20"))
ORDERBOOK_TIMEOUT = int(os.getenv("POLYMARKET_ORDERBOOK_TIMEOUT_SEC", "7"))
MARKET_SEARCH_QUERY = os.getenv("POLYMARKET_MARKET_SEARCH", "highest temperature").strip()
WEATHER_PAGE_URL = "https://polymarket.com/weather/temperature"
MANUAL_EVENT_URLS_RAW = os.getenv("POLYMARKET_EVENT_URLS", "").strip()
DEFAULT_EVENT_URLS = [
    "https://polymarket.com/event/highest-temperature-in-seoul-on-march-25-2026",
    "https://polymarket.com/event/highest-temperature-in-tokyo-on-march-25-2026",
    "https://polymarket.com/event/highest-temperature-in-shanghai-on-march-25-2026",
    "https://polymarket.com/event/highest-temperature-in-wellington-on-march-25-2026",
    "https://polymarket.com/event/highest-temperature-in-london-on-march-25-2026",
]

# =========================

session = requests.Session()
session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }
)
public_client = ClobClient(CLOB_API_URL, chain_id=CHAIN_ID)
trading_client: Optional[ClobClient] = None
geoblock_cache: Optional[dict] = None
_log_handle = None
weather_tag_ids_cache: Optional[List[str]] = None


class TeeWriter:
    def __init__(self, *streams) -> None:
        self.streams = streams

    def write(self, data: str) -> int:
        for stream in self.streams:
            stream.write(data)
            stream.flush()
        return len(data)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


def setup_logging() -> None:
    global _log_handle
    if DISABLE_FILE_LOG:
        return

    default_name = f"combined_{datetime.now(UTC).date().isoformat()}.log"
    log_path = Path(LOG_FILE) if LOG_FILE else Path("logs") / default_name
    log_path.parent.mkdir(parents=True, exist_ok=True)
    _log_handle = log_path.open("a", encoding="utf-8", buffering=1)
    sys.stdout = TeeWriter(sys.stdout, _log_handle)
    sys.stderr = TeeWriter(sys.stderr, _log_handle)
    print(f"Логирование в файл включено: {log_path.resolve()}")


def close_logging() -> None:
    global _log_handle
    if _log_handle is not None:
        _log_handle.close()
        _log_handle = None


def utc_today() -> str:
    return datetime.now(UTC).date().isoformat()


def target_dates() -> Set[str]:
    today = datetime.now(UTC).date()
    return {(today + timedelta(days=offset)).isoformat() for offset in range(LOOKAHEAD_DAYS)}


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


def get_day_state() -> dict:
    state = load_state()
    day = utc_today()
    day_state = state.get(day, {"buy_count": 0, "bought_token_ids": [], "stopped_out_token_ids": []})
    state[day] = day_state
    save_state(state)
    return day_state


def register_buy(token_id: str) -> None:
    state = load_state()
    day = utc_today()
    day_state = state.setdefault(day, {"buy_count": 0, "bought_token_ids": [], "stopped_out_token_ids": []})
    day_state["buy_count"] = int(day_state.get("buy_count", 0)) + 1
    if token_id not in day_state["bought_token_ids"]:
        day_state["bought_token_ids"].append(token_id)
    save_state(state)


def register_stop_loss(token_id: str) -> None:
    state = load_state()
    day = utc_today()
    day_state = state.setdefault(day, {"buy_count": 0, "bought_token_ids": [], "stopped_out_token_ids": []})
    if token_id not in day_state["stopped_out_token_ids"]:
        day_state["stopped_out_token_ids"].append(token_id)
    save_state(state)


def check_geoblock() -> dict:
    global geoblock_cache

    if geoblock_cache is not None:
        return geoblock_cache

    try:
        response = session.get("https://polymarket.com/api/geoblock", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        geoblock_cache = response.json()
    except Exception as error:
        geoblock_cache = {"blocked": False, "error": str(error)}

    return geoblock_cache


def normalize_private_key(raw_key: str) -> str:
    key = (raw_key or "").strip()
    if not key:
        return ""

    body = key[2:] if key.startswith("0x") else key
    if len(body) != 64 or any(char not in "0123456789abcdefABCDEF" for char in body):
        raise ValueError("PRIVATE_KEY должен быть hex-строкой длиной 64 символа.")
    return f"0x{body}"


def get_proxy_wallet(address: str) -> Optional[str]:
    try:
        response = session.get(
            PROFILE_API_URL,
            params={"address": address},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        proxy_wallet = payload.get("proxyWallet")
        return proxy_wallet.lower() if isinstance(proxy_wallet, str) and proxy_wallet else None
    except Exception as error:
        print(f"Не удалось определить proxy wallet для {address}: {error}")
        return None


def get_trading_client() -> ClobClient:
    global trading_client

    if trading_client is not None:
        return trading_client

    normalized_key = normalize_private_key(PRIVATE_KEY)
    if not normalized_key:
        raise ValueError("Для live-режима нужен PRIVATE_KEY signer-адреса.")

    wallet = (WALLET_ADDRESS or "").strip().lower()
    funder = FUNDER_ADDRESS or get_proxy_wallet(wallet) or wallet

    trading_client = ClobClient(
        CLOB_API_URL,
        key=normalized_key,
        chain_id=CHAIN_ID,
        signature_type=SIGNATURE_TYPE,
        funder=funder or None,
    )

    if hasattr(trading_client, "create_or_derive_api_creds") and hasattr(trading_client, "set_api_creds"):
        trading_client.set_api_creds(trading_client.create_or_derive_api_creds())

    return trading_client


def get_weather_tag_ids() -> List[str]:
    global weather_tag_ids_cache
    if weather_tag_ids_cache is not None:
        return weather_tag_ids_cache

    weather_tag_ids: List[str] = []
    offset = 0
    while True:
        try:
            response = session.get(
                GAMMA_TAGS_URL,
                params={"limit": 200, "offset": offset},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                batch = payload.get("data") or payload.get("tags") or []
            else:
                batch = payload
        except Exception:
            break
        if not batch:
            break
        for tag in batch:
            slug = str(tag.get("slug") or "").lower()
            label = str(tag.get("label") or "").lower()
            if "weather" in slug or "temperature" in slug or "weather" in label or "temperature" in label:
                try:
                    tag_id = tag.get("id") or tag.get("tagId") or tag.get("tag_id")
                    if tag_id is None:
                        continue
                    weather_tag_ids.append(str(tag_id))
                except Exception:
                    continue
        offset += 200
        if offset >= 1000:
            break

    weather_tag_ids_cache = list(dict.fromkeys(weather_tag_ids))
    return weather_tag_ids_cache




def optional_float(value: str) -> Optional[float]:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None

def safe_float(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def round_down(value: float, decimals: int = 2) -> float:
    factor = 10 ** decimals
    return math.floor(value * factor) / factor


def get_effective_size(position: dict) -> float:
    size = safe_float(position.get("size"))
    avg_price = safe_float(position.get("avgPrice"))
    cur_price = safe_float(position.get("curPrice"))
    current_value = safe_float(position.get("currentValue"))
    initial_value = safe_float(position.get("initialValue"))
    total_bought = safe_float(position.get("totalBought"))

    if cur_price > 0 and current_value > 0:
        return current_value / cur_price
    if avg_price > 0 and initial_value > 0:
        return initial_value / avg_price
    if size > 0:
        return size
    if avg_price > 0 and total_bought > 0:
        return total_bought / avg_price
    return 0.0


def is_weather_temperature_payload(payload: dict) -> bool:
    slug = (payload.get("slug") or "").lower().lstrip("/")
    title = (payload.get("title") or payload.get("question") or payload.get("description") or "").lower()
    event_slug = (payload.get("eventSlug") or "").lower().lstrip("/")
    events = payload.get("events") or []
    event_slugs = " ".join((str(event.get("slug") or "").lower().lstrip("/") for event in events if isinstance(event, dict)))
    event_titles = " ".join(
        (str(event.get("title") or event.get("question") or "").lower() for event in events if isinstance(event, dict))
    )
    full_text = f"{title} {event_titles}".strip()
    return (
        slug.startswith(WEATHER_SLUG_PREFIX)
        or event_slug.startswith(WEATHER_SLUG_PREFIX)
        or "weather/temperature" in event_slugs
        or any(keyword in full_text for keyword in WEATHER_KEYWORDS)
    )


def matches_target_dates(payload: dict) -> bool:
    targets = target_dates()
    candidate_end_dates = [str(payload.get("endDate") or "").strip()]
    events = payload.get("events") or []
    for event in events:
        if isinstance(event, dict):
            candidate_end_dates.append(str(event.get("endDate") or "").strip())

    for end_date_raw in candidate_end_dates:
        if not end_date_raw:
            continue
        end_date_key = end_date_raw[:10]
        for day_shift in range(DATE_SHIFT_TOLERANCE_DAYS + 1):
            try:
                shifted_key = (datetime.fromisoformat(end_date_key) - timedelta(days=day_shift)).date().isoformat()
                if shifted_key in targets:
                    return True
            except Exception:
                break

    title_parts = [str(payload.get("title") or payload.get("question") or "")]
    for event in events:
        if isinstance(event, dict):
            title_parts.append(str(event.get("title") or event.get("question") or ""))
    title = " ".join(part for part in title_parts if part).strip()
    match = re.search(
        r"(?:on|for)\s+([A-Za-z]{3,9})\.?\s+(\d{1,2})(?:,\s*(\d{4}))?",
        title,
        flags=re.IGNORECASE,
    )
    if not match:
        return SIMPLE_ENTRY_MODE

    month_name, day_raw, year_raw = match.groups()
    day_int = int(day_raw)
    if year_raw:
        try:
            parsed = datetime.strptime(f"{month_name} {day_int} {year_raw}", "%B %d %Y").date()
        except ValueError:
            try:
                parsed = datetime.strptime(f"{month_name} {day_int} {year_raw}", "%b %d %Y").date()
            except ValueError:
                return False
        return parsed.isoformat() in targets

    for target in targets:
        try:
            d = datetime.fromisoformat(target).date()
        except ValueError:
            continue
        if d.day == day_int and d.strftime("%B").lower() == month_name.lower():
            return True
        if d.day == day_int and d.strftime("%b").lower() == month_name.lower():
            return True

    return SIMPLE_ENTRY_MODE


def normalize_position(position: dict) -> Optional[Dict[str, float]]:
    size = get_effective_size(position)
    entry = safe_float(position.get("avgPrice"))
    token_id = str(position.get("asset") or position.get("tokenId") or "").strip()
    outcome = str(position.get("outcome") or "").strip().upper()

    if size < MIN_POSITION_SIZE or entry <= 0 or not token_id:
        return None

    return {
        "entry": entry,
        "size": size,
        "token_id": token_id,
        "title": position.get("title") or "Unknown market",
        "outcome": outcome,
        "cur_price": safe_float(position.get("curPrice")),
        "end_date": str(position.get("endDate") or ""),
    }


def fetch_positions_for_user(address: str) -> List[dict]:
    response = session.get(
        DATA_API_URL,
        params={
            "user": address,
            "sizeThreshold": 0,
            "redeemable": "false",
            "mergeable": "false",
            "limit": 500,
            "offset": 0,
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def get_positions() -> Dict[str, Dict[str, float]]:
    wallet = (WALLET_ADDRESS or "").strip().lower()
    if not wallet:
        raise ValueError("WALLET_ADDRESS пустой.")

    candidate_addresses = []
    seen_addresses = set()
    for address in (wallet, get_proxy_wallet(wallet)):
        if address and address not in seen_addresses:
            candidate_addresses.append(address)
            seen_addresses.add(address)

    positions: Dict[str, Dict[str, float]] = {}
    for address in candidate_addresses:
        raw_positions = fetch_positions_for_user(address)
        for raw_position in raw_positions:
            if not is_weather_temperature_payload(raw_position):
                continue
            if not matches_target_dates(raw_position):
                continue

            position = normalize_position(raw_position)
            if not position:
                continue
            if position["outcome"] != "YES":
                continue
            positions[position["token_id"]] = position

    return positions


def get_weather_markets_to_buy(strict_weather_filter: bool = True, query_mode: str = "search") -> List[dict]:
    def parse_list(value: object) -> list:
        if isinstance(value, list):
            return value
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        return []

    def extract_yes_outcomes(market: dict) -> List[dict]:
        extracted: List[dict] = []

        outcomes_raw = market.get("outcomes", [])
        if isinstance(outcomes_raw, list) and outcomes_raw and isinstance(outcomes_raw[0], dict):
            for outcome in outcomes_raw:
                if str(outcome.get("outcome") or "").strip().upper() != "YES":
                    continue
                token_id = str(outcome.get("token_id") or "").strip()
                if not token_id:
                    continue
                extracted.append({"token_id": token_id, "price": safe_float(outcome.get("price"))})
            return extracted

        tokens_raw = market.get("tokens") or market.get("clobTokens") or []
        if isinstance(tokens_raw, list):
            for token in tokens_raw:
                if not isinstance(token, dict):
                    continue
                outcome_name = str(token.get("outcome") or token.get("name") or "").strip().upper()
                if outcome_name != "YES":
                    continue
                token_id = str(
                    token.get("token_id")
                    or token.get("tokenId")
                    or token.get("asset")
                    or token.get("clobTokenId")
                    or ""
                ).strip()
                if not token_id:
                    continue
                extracted.append(
                    {
                        "token_id": token_id,
                        "price": safe_float(token.get("price") or token.get("lastPrice") or token.get("bestAsk")),
                    }
                )
            if extracted:
                return extracted

        outcome_names = [str(item).strip().upper() for item in parse_list(outcomes_raw)]
        token_ids = [str(item).strip() for item in parse_list(market.get("tokenIds"))]
        if not token_ids:
            token_ids = [str(item).strip() for item in parse_list(market.get("clobTokenIds"))]
        outcome_prices = parse_list(market.get("outcomePrices"))
        if not outcome_prices:
            outcome_prices = parse_list(market.get("prices"))

        for index, name in enumerate(outcome_names):
            if name != "YES":
                continue
            token_id = token_ids[index] if index < len(token_ids) else ""
            if not token_id:
                continue
            price_value = outcome_prices[index] if index < len(outcome_prices) else 0
            extracted.append({"token_id": token_id, "price": safe_float(price_value)})

        if extracted:
            return extracted

        # Fallback для бинарных рынков без имен outcomes.
        if len(token_ids) == 2:
            fallback_price = safe_float(market.get("price") or market.get("lastPrice") or market.get("bestAsk"))
            extracted.append({"token_id": token_ids[0], "price": fallback_price})

        return extracted

    if query_mode == "events_by_tag":
        markets: List[dict] = []
        scanned_total = 0
        skipped_non_weather = 0
        skipped_non_target_date = 0
        skipped_no_yes_outcome = 0
        started = time.monotonic()
        weather_tag_ids = get_weather_tag_ids()
        if not weather_tag_ids:
            print("BUY SCAN EVENTS_BY_TAG: не удалось определить weather tag ids, переходим к query_mode=event_slug.")
            return get_weather_markets_to_buy(strict_weather_filter=True, query_mode="event_slug")

        for tag_id in weather_tag_ids:
            offset = 0
            while (time.monotonic() - started) <= BUY_SCAN_MAX_DURATION_SEC:
                try:
                    response = session.get(
                        GAMMA_EVENTS_URL,
                        params={"tag_id": tag_id, "active": "true", "closed": "false", "limit": 100, "offset": offset},
                        timeout=REQUEST_TIMEOUT,
                    )
                    response.raise_for_status()
                    events_batch = response.json()
                except Exception:
                    break
                if not events_batch:
                    break

                for event in events_batch:
                    event_markets = event.get("markets") or []
                    for market in event_markets:
                        if not isinstance(market, dict):
                            continue
                        scanned_total += 1
                        if not is_weather_temperature_payload(market):
                            skipped_non_weather += 1
                            continue
                        if not matches_target_dates(market):
                            skipped_non_target_date += 1
                            continue
                        outcomes = extract_yes_outcomes(market)
                        if not outcomes:
                            skipped_no_yes_outcome += 1
                            continue
                        for outcome in outcomes:
                            markets.append(
                                {
                                    "title": market.get("question") or market.get("title") or "Unknown market",
                                    "token_id": outcome.get("token_id", ""),
                                    "price": safe_float(outcome.get("price")),
                                    "end_date": str(market.get("endDate") or event.get("endDate") or ""),
                                    "volume": safe_float(market.get("volume") or market.get("volumeNum")),
                                }
                            )
                            if len(markets) >= MAX_MARKETS_PER_SCAN:
                                break
                        if len(markets) >= MAX_MARKETS_PER_SCAN:
                            break
                    if len(markets) >= MAX_MARKETS_PER_SCAN:
                        break
                if len(markets) >= MAX_MARKETS_PER_SCAN:
                    break
                offset += 100
            if len(markets) >= MAX_MARKETS_PER_SCAN:
                break

        print(
            "BUY SCAN FILTERS | "
            "strict_weather_filter=True "
            "query_mode=events_by_tag "
            f"scanned={scanned_total} "
            f"skip_non_weather={skipped_non_weather} "
            f"skip_non_target_date={skipped_non_target_date} "
            f"skip_no_yes_outcome={skipped_no_yes_outcome} "
            f"candidates={len(markets)}"
        )
        return markets

    if query_mode == "site_events":
        markets: List[dict] = []
        scanned_total = 0
        skipped_non_weather = 0
        skipped_non_target_date = 0
        skipped_no_yes_outcome = 0
        manual_event_urls = [url.strip() for url in MANUAL_EVENT_URLS_RAW.split(",") if url.strip()]
        if not manual_event_urls:
            manual_event_urls = DEFAULT_EVENT_URLS.copy()
        market_slugs: List[str] = []

        if manual_event_urls:
            for event_url in manual_event_urls[:80]:
                try:
                    event_html = session.get(event_url, timeout=REQUEST_TIMEOUT).text
                except Exception:
                    continue
                nested_market_slugs = re.findall(r"(?:market=|/market/)([a-z0-9-]+)", event_html.lower())
                for nested_slug in nested_market_slugs:
                    if nested_slug not in market_slugs:
                        market_slugs.append(nested_slug)
        else:
            try:
                page_html = session.get(WEATHER_PAGE_URL, timeout=REQUEST_TIMEOUT).text
            except Exception as error:
                print(f"BUY SCAN SITE_EVENTS ERROR: не удалось загрузить {WEATHER_PAGE_URL}: {error}")
                return markets

            market_slug_matches = re.findall(r"(?:market=|/market/)([a-z0-9-]+)", page_html.lower())
            market_slugs = list(dict.fromkeys(market_slug_matches))

            if not market_slugs:
                event_slug_matches = re.findall(r"/event/(highest-temperature-[a-z0-9-]+)", page_html.lower())
                event_slugs = list(dict.fromkeys(event_slug_matches))
                for event_slug in event_slugs[:60]:
                    try:
                        event_html = session.get(f"https://polymarket.com/event/{event_slug}", timeout=REQUEST_TIMEOUT).text
                    except Exception:
                        continue
                    nested_market_slugs = re.findall(r"(?:market=|/market/)([a-z0-9-]+)", event_html.lower())
                    for nested_slug in nested_market_slugs:
                        if nested_slug not in market_slugs:
                            market_slugs.append(nested_slug)

        if not market_slugs:
            print("BUY SCAN SITE_EVENTS: на странице weather/temperature не найдено market slug.")
            return markets

        started = time.monotonic()
        for market_slug in market_slugs:
            if (time.monotonic() - started) > BUY_SCAN_MAX_DURATION_SEC:
                break
            try:
                response = session.get(
                    GAMMA_MARKETS_URL,
                    params={"slug": market_slug, "closed": "false", "archived": "false", "limit": 100, "offset": 0},
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                batch = response.json()
            except Exception:
                continue

            for market in batch:
                scanned_total += 1
                if not is_weather_temperature_payload(market):
                    skipped_non_weather += 1
                    continue
                if not matches_target_dates(market):
                    skipped_non_target_date += 1
                    continue
                outcomes = extract_yes_outcomes(market)
                if not outcomes:
                    skipped_no_yes_outcome += 1
                    continue
                for outcome in outcomes:
                    markets.append(
                        {
                            "title": market.get("question") or market.get("title") or "Unknown market",
                            "token_id": outcome.get("token_id", ""),
                            "price": safe_float(outcome.get("price")),
                            "end_date": str(market.get("endDate") or ""),
                            "volume": safe_float(market.get("volume") or market.get("volumeNum")),
                        }
                    )
                    if len(markets) >= MAX_MARKETS_PER_SCAN:
                        break
                if len(markets) >= MAX_MARKETS_PER_SCAN:
                    break
            if len(markets) >= MAX_MARKETS_PER_SCAN:
                break

        print(
            "BUY SCAN FILTERS | "
            "strict_weather_filter=True "
            "query_mode=site_events "
            f"scanned={scanned_total} "
            f"skip_non_weather={skipped_non_weather} "
            f"skip_non_target_date={skipped_non_target_date} "
            f"skip_no_yes_outcome={skipped_no_yes_outcome} "
            f"candidates={len(markets)}"
        )
        return markets

    markets: List[dict] = []
    offset = 0
    scan_started = time.monotonic()
    progress_line_active = False
    scanned_total = 0
    skipped_non_weather = 0
    skipped_non_target_date = 0
    skipped_no_yes_outcome = 0

    while True:
        elapsed = time.monotonic() - scan_started
        if elapsed > BUY_SCAN_MAX_DURATION_SEC:
            if progress_line_active:
                print()
            print(
                f"BUY SCAN TIMEBOX: достигли лимита {BUY_SCAN_MAX_DURATION_SEC}s, "
                f"собрано рынков={len(markets)}"
            )
            break

        print(f"\rBUY SCAN PROGRESS | offset={offset} elapsed={elapsed:.1f}s", end="", flush=True)
        progress_line_active = True
        params = {
            "limit": 100,
            "offset": offset,
            "closed": "false",
            "archived": "false",
        }
        if query_mode == "event_slug":
            params["eventSlug"] = WEATHER_SLUG_PREFIX
        elif query_mode == "slug":
            params["slug"] = WEATHER_SLUG_PREFIX
        elif MARKET_SEARCH_QUERY:
            params["search"] = MARKET_SEARCH_QUERY

        response = session.get(
            GAMMA_MARKETS_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            if progress_line_active:
                print()
            break

        for market in batch:
            scanned_total += 1
            if strict_weather_filter:
                if not is_weather_temperature_payload(market):
                    skipped_non_weather += 1
                    continue
            else:
                weak_text = (
                    f"{market.get('question') or ''} {market.get('title') or ''} "
                    f"{market.get('slug') or ''} {market.get('eventSlug') or ''}"
                ).lower()
                if "highest temperature" not in weak_text and "highest-temp" not in weak_text:
                    skipped_non_weather += 1
                    continue
            if not matches_target_dates(market):
                skipped_non_target_date += 1
                continue

            outcomes = extract_yes_outcomes(market)
            if not outcomes:
                skipped_no_yes_outcome += 1
                continue

            for outcome in outcomes:
                markets.append(
                    {
                        "title": market.get("question") or market.get("title") or "Unknown market",
                        "token_id": outcome.get("token_id", ""),
                        "price": safe_float(outcome.get("price")),
                        "end_date": str(market.get("endDate") or ""),
                        "volume": safe_float(market.get("volume") or market.get("volumeNum")),
                    }
                )
                if len(markets) >= MAX_MARKETS_PER_SCAN:
                    if progress_line_active:
                        print()
                    return markets

        offset += 100

    if progress_line_active:
        print()
    print(
        "BUY SCAN FILTERS | "
        f"strict_weather_filter={strict_weather_filter} "
        f"query_mode={query_mode} "
        f"scanned={scanned_total} "
        f"skip_non_weather={skipped_non_weather} "
        f"skip_non_target_date={skipped_non_target_date} "
        f"skip_no_yes_outcome={skipped_no_yes_outcome} "
        f"candidates={len(markets)}"
    )
    if strict_weather_filter and not markets:
        if query_mode == "search":
            print("BUY SCAN FALLBACK: search не дал weather-рынки, повторяем query_mode=events_by_tag.")
            return get_weather_markets_to_buy(strict_weather_filter=True, query_mode="events_by_tag")
        if query_mode == "events_by_tag":
            print("BUY SCAN FALLBACK: events_by_tag не дал weather-рынки, повторяем query_mode=event_slug.")
            return get_weather_markets_to_buy(strict_weather_filter=True, query_mode="event_slug")
        if query_mode == "event_slug":
            print("BUY SCAN FALLBACK: event_slug не дал weather-рынки, повторяем query_mode=slug.")
            return get_weather_markets_to_buy(strict_weather_filter=True, query_mode="slug")
        if query_mode == "slug":
            print("BUY SCAN FALLBACK: slug не дал weather-рынки, пробуем query_mode=site_events.")
            return get_weather_markets_to_buy(strict_weather_filter=True, query_mode="site_events")
        print("BUY SCAN FALLBACK: weather filter не сработал, повторяем скан с ослабленным temp-фильтром.")
        return get_weather_markets_to_buy(strict_weather_filter=False, query_mode="search")
    return markets


def get_best_bid_price(token_id: str) -> Optional[float]:
    result: dict = {}
    error: dict = {}

    def worker() -> None:
        try:
            result["book"] = public_client.get_order_book(token_id)
        except Exception as inner_error:
            error["value"] = inner_error

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(ORDERBOOK_TIMEOUT)

    if thread.is_alive():
        print(f"ORDERBOOK TIMEOUT: token={token_id} side=bid timeout={ORDERBOOK_TIMEOUT}s")
        return None

    if "value" in error:
        print(f"Не удалось получить bid для {token_id}: {error['value']}")
        return None

    try:
        book = result.get("book")
        return float(book.bids[0].price) if book.bids else None
    except Exception as error:
        print(f"Не удалось получить bid для {token_id}: {error}")
        return None


def get_best_ask_price(token_id: str) -> Optional[float]:
    result: dict = {}
    error: dict = {}

    def worker() -> None:
        try:
            result["book"] = public_client.get_order_book(token_id)
        except Exception as inner_error:
            error["value"] = inner_error

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    thread.join(ORDERBOOK_TIMEOUT)

    if thread.is_alive():
        print(f"ORDERBOOK TIMEOUT: token={token_id} side=ask timeout={ORDERBOOK_TIMEOUT}s")
        return None

    if "value" in error:
        print(f"Не удалось получить ask для {token_id}: {error['value']}")
        return None

    try:
        book = result.get("book")
        return float(book.asks[0].price) if book.asks else None
    except Exception as error:
        print(f"Не удалось получить ask для {token_id}: {error}")
        return None


def get_price_history_points(token_id: str, lookback_sec: int) -> List[tuple[int, float]]:
    now_ts = int(datetime.now(UTC).timestamp())
    start_ts = max(0, now_ts - lookback_sec)

    param_variants = [
        {"market": token_id, "startTs": start_ts, "endTs": now_ts, "fidelity": 5},
        {"market": token_id, "start": start_ts, "end": now_ts, "fidelity": 5},
        {"token_id": token_id, "startTs": start_ts, "endTs": now_ts, "fidelity": 5},
    ]

    for params in param_variants:
        try:
            response = session.get(f"{CLOB_API_URL}/prices-history", params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code >= 400:
                continue
            payload = response.json()
        except Exception:
            continue

        raw_points = []
        if isinstance(payload, dict):
            for key in ("history", "prices", "data"):
                if isinstance(payload.get(key), list):
                    raw_points = payload[key]
                    break
        elif isinstance(payload, list):
            raw_points = payload

        points: List[tuple[int, float]] = []
        for point in raw_points:
            if isinstance(point, dict):
                ts = point.get("t") or point.get("ts") or point.get("timestamp")
                px = point.get("p") or point.get("price") or point.get("value")
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                ts, px = point[0], point[1]
            else:
                continue

            try:
                ts_i = int(float(ts))
                px_f = float(px)
            except (TypeError, ValueError):
                continue

            if px_f > 0:
                points.append((ts_i, px_f))

        if points:
            points.sort(key=lambda x: x[0])
            return points

    return []


def get_price_at_or_before(points: List[tuple[int, float]], target_ts: int) -> Optional[float]:
    candidate = None
    for ts, price in points:
        if ts <= target_ts:
            candidate = price
        else:
            break
    return candidate


def passes_buy_trend_filters(token_id: str, current_price: float) -> bool:
    points = get_price_history_points(token_id, MOMENTUM_LOOKBACK_6H_SEC)
    if not points:
        print(f"{token_id} | trend skip | нет price history")
        return False

    now_ts = int(datetime.now(UTC).timestamp())
    one_hour_ago_price = get_price_at_or_before(points, now_ts - MOMENTUM_LOOKBACK_1H_SEC)
    if one_hour_ago_price is None:
        print(f"{token_id} | trend skip | нет точки 1h назад")
        return False

    if current_price <= one_hour_ago_price:
        print(
            f"{token_id} | trend skip | текущая цена {current_price:.2f} <= цена 1h назад {one_hour_ago_price:.2f}"
        )
        return False

    if not SIMPLE_ENTRY_MODE:
        local_max = max(price for _, price in points)
        if local_max <= 0:
            return False

        pullback = (local_max - current_price) / local_max
        if pullback > MAX_PULLBACK_FROM_6H_HIGH:
            print(
                f"{token_id} | trend skip | просадка от 6h high {pullback:.1%} > {MAX_PULLBACK_FROM_6H_HIGH:.1%}"
            )
            return False

    return True


def should_take_profit(entry_price: float, current_price: float) -> bool:
    return current_price >= entry_price * (1 + TAKE_PROFIT_PCT)


def should_stop_loss(entry_price: float, current_price: float) -> bool:
    drawdown_price = entry_price * (1 - MAX_DRAWDOWN)
    absolute_stop = entry_price >= ABSOLUTE_SELL_PRICE and current_price <= ABSOLUTE_SELL_PRICE
    return current_price <= drawdown_price or absolute_stop


def buy(token_id: str, price: float) -> None:
    if DRY_RUN:
        print(f"DRY RUN: BUY token={token_id} price={price:.2f} usd={BUY_SIZE_USD:.2f}")
        return

    geo = check_geoblock()
    if geo.get("blocked"):
        print(
            "BUY BLOCKED: Polymarket geoblock запрещает отправку ордера для текущего IP. "
            f"country={geo.get('country')} region={geo.get('region')}"
        )
        return

    try:
        client = get_trading_client()
        buy_size = round_down(BUY_SIZE_USD / price, 2)
        if buy_size <= 0:
            print(f"BUY SKIPPED: слишком маленький размер для {token_id}")
            return

        order = OrderArgs(
            token_id=token_id,
            price=round(min(price + 0.01, 0.99), 2),
            size=buy_size,
            side="BUY",
        )
        signed = client.create_order(order)
        response = client.post_order(signed, OrderType.GTC)
        print(
            f"BUY: token={token_id} ask={price:.2f} usd={BUY_SIZE_USD:.2f} shares={buy_size:.2f} "
            f"status={response.get('status') if isinstance(response, dict) else None} "
            f"success={response.get('success') if isinstance(response, dict) else None}"
        )
        print(response)
        if isinstance(response, dict) and response.get("success"):
            register_buy(token_id)
    except Exception as error:
        print(f"BUY ERROR: не удалось отправить buy-ордер для {token_id}: {error}")


def sell(token_id: str, price: float, size: float, reason: str) -> None:
    if DRY_RUN:
        print(f"DRY RUN: SELL token={token_id} reason={reason} bid={price:.2f} size={size:.2f}")
        return

    geo = check_geoblock()
    if geo.get("blocked"):
        print(
            "SELL BLOCKED: Polymarket geoblock запрещает отправку ордера для текущего IP. "
            f"country={geo.get('country')} region={geo.get('region')}"
        )
        return

    try:
        client = get_trading_client()
        sell_size = round_down(size * SELL_SIZE_BUFFER, 2)
        if sell_size <= 0:
            print(f"SELL SKIPPED: размер после буфера слишком мал для {token_id}")
            return

        order = OrderArgs(
            token_id=token_id,
            price=round(max(price - 0.01, 0.01), 2),
            size=sell_size,
            side="SELL",
        )
        signed = client.create_order(order)
        response = client.post_order(signed, OrderType.GTC)
        print(
            f"SELL: token={token_id} reason={reason} bid={price:.2f} requested_size={size:.2f} sent_size={sell_size:.2f} "
            f"status={response.get('status') if isinstance(response, dict) else None} "
            f"success={response.get('success') if isinstance(response, dict) else None}"
        )
        print(response)
        if isinstance(response, dict) and response.get("success") and reason == "stop_loss":
            register_stop_loss(token_id)
    except Exception as error:
        print(f"SELL ERROR: не удалось отправить sell-ордер для {token_id}: {error}")


# =========================
# MAIN LOOP
# =========================

setup_logging()
atexit.register(close_logging)
print(
    "Конфиг цикла: "
    f"check_interval={CHECK_INTERVAL}s request_timeout={REQUEST_TIMEOUT}s "
    f"orderbook_timeout={ORDERBOOK_TIMEOUT}s max_markets_per_scan={MAX_MARKETS_PER_SCAN} "
    f"buy_scan_max_duration={BUY_SCAN_MAX_DURATION_SEC}s enable_buy_scan={ENABLE_BUY_SCAN} "
    f"lookahead_days={LOOKAHEAD_DAYS} date_shift_tolerance={DATE_SHIFT_TOLERANCE_DAYS} "
    f"min_market_volume={MIN_MARKET_VOLUME_USD:.0f} simple_entry_mode={SIMPLE_ENTRY_MODE} "
    f"market_search='{MARKET_SEARCH_QUERY}' manual_event_urls={len([u for u in MANUAL_EVENT_URLS_RAW.split(',') if u.strip()]) or len(DEFAULT_EVENT_URLS)}"
)

while True:
    scan_started_at = datetime.now(UTC)
    print("\n=== WEATHER COMBINED BUY/SELL SCAN ===")

    try:
        positions = get_positions()
        day_state = get_day_state()
        stopped_out_today = set(day_state.get("stopped_out_token_ids", []))
        print(
            f"Открытых YES-позиций на {sorted(target_dates())}: {len(positions)} | "
            f"buy_count_today={day_state.get('buy_count', 0)}/{MAX_NEW_BUYS_PER_DAY}"
        )

        # 1) Выходы: take-profit и stop-loss для уже открытых позиций.
        for token_id, position in positions.items():
            entry = position["entry"]
            size = position["size"]
            current_price = position["cur_price"] or 0
            bid_price = get_best_bid_price(token_id)
            effective_price = current_price if current_price > 0 else (bid_price or 0)
            take_profit_price = entry * (1 + TAKE_PROFIT_PCT)
            stop_loss_price = entry * (1 - MAX_DRAWDOWN)

            if effective_price <= 0:
                print(f"{position['title']} | нет цены для проверки")
                continue

            if should_take_profit(entry, effective_price):
                print(
                    f"{position['title']} | TAKE PROFIT SIGNAL | entry={entry:.2f} cur={effective_price:.2f} "
                    f"target={take_profit_price:.2f} size={size:.2f}"
                )
                if bid_price is None:
                    print(f"WAIT: нет bid для take-profit закрытия {token_id}")
                    continue
                sell(token_id, bid_price, size, reason="take_profit")
                continue

            if should_stop_loss(entry, effective_price):
                print(
                    f"{position['title']} | STOP LOSS SIGNAL | entry={entry:.2f} cur={effective_price:.2f} "
                    f"stop20={stop_loss_price:.2f} abs_stop={ABSOLUTE_SELL_PRICE:.2f} size={size:.2f}"
                )
                if bid_price is None:
                    print(f"WAIT: нет bid для stop-loss закрытия {token_id}")
                    continue
                sell(token_id, bid_price, size, reason="stop_loss")
                continue

            print(
                f"{position['title']} | держим | entry={entry:.2f} cur={effective_price:.2f} "
                f"tp={take_profit_price:.2f} sl20={stop_loss_price:.2f} size={size:.2f}"
            )

        # 2) Входы: новые покупки только если дневной лимит ещё не исчерпан.
        if not ENABLE_BUY_SCAN:
            print("BUY SCAN DISABLED: пропускаем блок новых входов (POLYMARKET_ENABLE_BUY_SCAN=0)")
        else:
            print("BUY SCAN START")
            markets = get_weather_markets_to_buy()
            print(f"BUY SCAN CANDIDATES: {len(markets)}")
            existing_token_ids = set(positions.keys())

            for market in markets:
                if day_state.get("buy_count", 0) >= MAX_NEW_BUYS_PER_DAY:
                    print(f"BUY LIMIT REACHED: {MAX_NEW_BUYS_PER_DAY} новых покупок на {utc_today()}")
                    break

                token_id = market["token_id"]
                if token_id in existing_token_ids or token_id in stopped_out_today:
                    continue

                ask_price = get_best_ask_price(token_id)
                bid_price = get_best_bid_price(token_id)
                effective_buy_price = ask_price if ask_price is not None else market["price"]
                if effective_buy_price <= 0:
                    continue

                if not SIMPLE_ENTRY_MODE:
                    market_volume = market.get("volume", 0)
                    if market_volume < MIN_MARKET_VOLUME_USD:
                        print(
                            f"{market['title']} | buy skip | low volume {market_volume:.0f} < {MIN_MARKET_VOLUME_USD:.0f}"
                        )
                        continue

                    if bid_price is None or bid_price <= 0:
                        print(f"{market['title']} | buy skip | нет bid для расчета spread")
                        continue

                    spread_cents = (effective_buy_price - bid_price) * 100
                    if spread_cents > MAX_SPREAD_CENTS:
                        print(
                            f"{market['title']} | buy skip | spread {spread_cents:.1f}c > {MAX_SPREAD_CENTS:.1f}c "
                            f"(ask={effective_buy_price:.2f}, bid={bid_price:.2f})"
                        )
                        continue

                    max_spread_pct = optional_float(MAX_SPREAD_PCT_RAW)
                    if max_spread_pct is not None:
                        mid_price = (effective_buy_price + bid_price) / 2
                        spread_pct = ((effective_buy_price - bid_price) / mid_price) if mid_price > 0 else 1
                        if spread_pct > max_spread_pct:
                            print(
                                f"{market['title']} | buy skip | spread {spread_pct:.1%} > {max_spread_pct:.1%} "
                                f"(ask={effective_buy_price:.2f}, bid={bid_price:.2f})"
                            )
                            continue

                if BUY_MIN_PRICE <= effective_buy_price <= BUY_MAX_PRICE:
                    if not passes_buy_trend_filters(token_id, effective_buy_price):
                        print(
                            f"{market['title']} | buy skip | trend filter | ask={effective_buy_price:.2f} "
                            f"range={BUY_MIN_PRICE:.2f}-{BUY_MAX_PRICE:.2f} end={market['end_date'][:10]}"
                        )
                        continue

                    print(
                        f"{market['title']} | BUY SIGNAL | ask={effective_buy_price:.2f} "
                        f"budget=${BUY_SIZE_USD:.2f} end={market['end_date'][:10]}"
                    )
                    buy(token_id, effective_buy_price)
                    day_state = get_day_state()
                    stopped_out_today = set(day_state.get("stopped_out_token_ids", []))
                else:
                    print(
                        f"{market['title']} | buy skip | ask={effective_buy_price:.2f} "
                        f"range={BUY_MIN_PRICE:.2f}-{BUY_MAX_PRICE:.2f} end={market['end_date'][:10]}"
                    )

    except Exception as error:
        print(f"Ошибка цикла мониторинга: {error}")

    scan_finished_at = datetime.now(UTC)
    scan_duration_sec = (scan_finished_at - scan_started_at).total_seconds()
    print(
        f"=== SCAN COMPLETE | started={scan_started_at.isoformat()} "
        f"finished={scan_finished_at.isoformat()} duration={scan_duration_sec:.1f}s "
        f"| sleep={CHECK_INTERVAL}s ==="
    )
    time.sleep(CHECK_INTERVAL)
