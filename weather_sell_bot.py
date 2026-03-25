import math
import os
import re
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Set

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

# =========================
# НАСТРОЙКИ
# =========================

# PRIVATE_KEY = приватный hex-ключ signer address (не Relayer API key).
# Должен быть строкой вида 0xabc123... или abc123... и содержать только hex-символы.
PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")

# WALLET_ADDRESS = адрес для поиска позиций на Polymarket.
# Обычно сюда удобно ставить адрес из настроек Polymarket / Signer Address.
# Бот дополнительно сам попытается найти proxy wallet через public-profile API.
WALLET_ADDRESS = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip().lower()
# Для большинства обычных Polymarket.com аккаунтов корректнее proxy signature type 2.
# При необходимости можно переопределить через POLYMARKET_SIGNATURE_TYPE.
SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))

CHAIN_ID = 137
CHECK_INTERVAL = int(os.getenv("POLYMARKET_CHECK_INTERVAL_SEC", "10"))
DRY_RUN = True

# Необязательный фильтр по дате экспирации (UTC).
# По умолчанию он выключен: бот берёт все текущие открытые позиции из /positions.
# Если нужно, можно вручную задать POLYMARKET_TARGET_DATE_UTC=2026-03-21.
TARGET_DATE_UTC = os.getenv("POLYMARKET_TARGET_DATE_UTC", "").strip()

# Пыль/остатки фильтруются по минимальному размеру позиции.
# По умолчанию отсекаем совсем крошечные хвосты (< 0.02), которые часто не выглядят как рабочие позиции в UI.
# При необходимости можно переопределить через POLYMARKET_MIN_POSITION_SIZE.
MIN_POSITION_SIZE = float(os.getenv("POLYMARKET_MIN_POSITION_SIZE", "0.02"))

# BUY-настройки (добавлены поверх sell-бота).
LOOKAHEAD_DAYS = int(os.getenv("POLYMARKET_LOOKAHEAD_DAYS", "1"))
# В Polymarket цена YES хранится как доля от $1 за share (0.00..1.00).
# Пример: 0.35 = 35 центов = 35%.
BUY_MIN_PRICE = float(os.getenv("POLYMARKET_BUY_MIN_PRICE", "0.35"))
BUY_MAX_PRICE = float(os.getenv("POLYMARKET_BUY_MAX_PRICE", "0.65"))
BUY_SIZE_USD = float(os.getenv("POLYMARKET_BUY_SIZE_USD", "1.0"))
MAX_SPREAD_CENTS = float(os.getenv("POLYMARKET_MAX_SPREAD_CENTS", "10"))
MAX_NEW_BUYS_PER_DAY = int(os.getenv("POLYMARKET_MAX_NEW_BUYS_PER_DAY", "50"))
BUY_SCAN_MAX_DURATION_SEC = int(os.getenv("POLYMARKET_BUY_SCAN_MAX_DURATION_SEC", "60"))
BUY_MAX_CANDIDATES_PER_CITY = int(os.getenv("POLYMARKET_BUY_MAX_CANDIDATES_PER_CITY", "2"))
BUY_MAX_CANDIDATES_TOTAL = int(os.getenv("POLYMARKET_BUY_MAX_CANDIDATES_TOTAL", "0"))
STATE_FILE = Path(os.getenv("POLYMARKET_STATE_FILE", "weather_sell_bot_state.json"))

# Продаём позицию, если текущая цена упала на 25%+ от цены входа.
MAX_DRAWDOWN = float(os.getenv("POLYMARKET_MAX_DRAWDOWN", "0.20"))

# Или если цена YES-исхода опустилась до 30 центов ($0.30) и ниже.
ABSOLUTE_SELL_PRICE = float(os.getenv("POLYMARKET_ABSOLUTE_SELL_PRICE", "0.25"))

# Take-profit: закрываем позицию при росте цены на 30%+ от входа.
TAKE_PROFIT_PCT = 0.30

# Небольшой буфер, чтобы не пытаться продать дробный остаток больше доступного баланса.
SELL_SIZE_BUFFER = 0.995

# Ограничиваемся только рынками погоды / температур.
WEATHER_SLUG_PREFIX = "weather/temperature"
WEATHER_KEYWORDS = (
    "highest temperature",
    "temperature",
)

BUY_REQUIRED_QUESTION_PHRASE = os.getenv("POLYMARKET_BUY_REQUIRED_QUESTION_PHRASE", "will the highest temperature in").strip().lower()
BUY_REQUIRE_QUESTION_PHRASE = os.getenv("POLYMARKET_BUY_REQUIRE_QUESTION_PHRASE", "1").strip().lower() not in {"0", "false", "no"}
USE_HOURLY_MOMENTUM_FILTER = os.getenv("POLYMARKET_USE_HOURLY_MOMENTUM_FILTER", "0").strip().lower() in {"1", "true", "yes"}
USE_SPREAD_FILTER = os.getenv("POLYMARKET_USE_SPREAD_FILTER", "1").strip().lower() not in {"0", "false", "no"}

DATA_API_URL = "https://data-api.polymarket.com/positions"
PROFILE_API_URL = "https://gamma-api.polymarket.com/public-profile"
CLOB_PRICES_HISTORY_URL = "https://clob.polymarket.com/prices-history"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_API_URL = "https://clob.polymarket.com"
REQUEST_TIMEOUT = 20

# =========================

session = requests.Session()
public_client = ClobClient(CLOB_API_URL, chain_id=CHAIN_ID)
trading_client: Optional[ClobClient] = None
geoblock_cache: Optional[dict] = None


def utc_today() -> str:
    return datetime.now(UTC).date().isoformat()


def normalize_target_date(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    # Поддержка случая, когда по ошибке передали строку формата
    # "POLYMARKET_TARGET_DATE_UTC=2026-03-25" как само значение переменной.
    if "=" in raw:
        raw = raw.split("=", 1)[1].strip()
    return raw[:10]


def target_dates() -> Set[str]:
    normalized_target = normalize_target_date(TARGET_DATE_UTC)
    if normalized_target:
        return {normalized_target}

    today = datetime.now(UTC).date()
    lookahead = max(1, LOOKAHEAD_DAYS)
    return {(today + timedelta(days=offset)).isoformat() for offset in range(lookahead)}


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
    day_state = state.get(day, {"buy_count": 0, "stopped_out_token_ids": []})
    state[day] = day_state
    save_state(state)
    return day_state


def register_buy(token_id: str) -> None:
    state = load_state()
    day = utc_today()
    day_state = state.setdefault(day, {"buy_count": 0, "stopped_out_token_ids": []})
    day_state["buy_count"] = int(day_state.get("buy_count", 0)) + 1
    save_state(state)


def register_stop_loss(token_id: str) -> None:
    state = load_state()
    day = utc_today()
    day_state = state.setdefault(day, {"buy_count": 0, "stopped_out_token_ids": []})
    token_ids = day_state.setdefault("stopped_out_token_ids", [])
    if token_id not in token_ids:
        token_ids.append(token_id)
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

    if key.startswith("0x"):
        body = key[2:]
    else:
        body = key

    if len(body) != 64 or any(char not in "0123456789abcdefABCDEF" for char in body):
        raise ValueError(
            "PRIVATE_KEY должен быть hex-строкой длиной 64 символа (с optional префиксом 0x). "
            "Relayer API key из настроек сюда не подходит."
        )

    return f"0x{body}"


def get_trading_client() -> ClobClient:
    global trading_client

    if trading_client is not None:
        return trading_client

    normalized_key = normalize_private_key(PRIVATE_KEY)
    if not normalized_key:
        raise ValueError(
            "Для реальной продажи нужен PRIVATE_KEY signer-адреса. Сейчас он пустой."
        )

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


def get_proxy_wallet(address: str) -> Optional[str]:
    """Возвращает proxy wallet для адреса пользователя, если он есть."""
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


def get_target_end_dates() -> set[str]:
    return {TARGET_DATE_UTC} if TARGET_DATE_UTC else set()


def is_weather_temperature_position(position: dict) -> bool:
    slug = (position.get("slug") or "").lower().lstrip("/")
    title = (position.get("title") or "").lower()
    question = (position.get("question") or "").lower()
    event_slug = (position.get("eventSlug") or "").lower().lstrip("/")

    return (
        slug.startswith(WEATHER_SLUG_PREFIX)
        or event_slug.startswith(WEATHER_SLUG_PREFIX)
        or any(keyword in title or keyword in question for keyword in WEATHER_KEYWORDS)
    )


def is_target_date_position(position: dict) -> bool:
    target_dates = get_target_end_dates()
    if not target_dates:
        return True

    end_date_raw = str(position.get("endDate") or "").strip()
    if not end_date_raw:
        return False

    end_date = end_date_raw[:10]
    return end_date in target_dates




def safe_float(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def get_effective_size(position: dict) -> float:
    size = safe_float(position.get("size"))
    avg_price = safe_float(position.get("avgPrice"))
    cur_price = safe_float(position.get("curPrice"))
    current_value = safe_float(position.get("currentValue"))
    initial_value = safe_float(position.get("initialValue"))
    total_bought = safe_float(position.get("totalBought"))

    # Для активной позиции UI ближе всего к количеству shares формула currentValue / curPrice.
    if cur_price > 0 and current_value > 0:
        return current_value / cur_price

    # Если currentValue нет, используем стартовую стоимость позиции.
    if avg_price > 0 and initial_value > 0:
        return initial_value / avg_price

    # Fallback на raw size из API.
    if size > 0:
        return size

    # Последний резервный вариант — восстановить размер из totalBought.
    if avg_price > 0 and total_bought > 0:
        return total_bought / avg_price

    return 0.0

def normalize_position(position: dict) -> Optional[Dict[str, float]]:
    size = get_effective_size(position)
    entry = safe_float(position.get("avgPrice"))

    if size < MIN_POSITION_SIZE:
        return None

    # В Data API ID позиции лежит в поле asset, а не tokenId.
    token_id = str(position.get("asset") or position.get("tokenId") or "").strip()
    outcome = str(position.get("outcome") or "").strip().upper()

    if size <= 0 or entry <= 0 or not token_id:
        return None

    return {
        "entry": entry,
        "size": size,
        "token_id": token_id,
        "title": position.get("title") or "Unknown market",
        "slug": position.get("slug") or "",
        "event_slug": position.get("eventSlug") or "",
        "outcome": outcome,
        "cur_price": float(position.get("curPrice", 0) or 0),
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
    """Загружает позиции по EOA и proxy wallet, затем оставляет только погодные YES позиции."""
    candidate_addresses = []
    seen_addresses = set()

    wallet = (WALLET_ADDRESS or "").strip().lower()
    if not wallet:
        raise ValueError("WALLET_ADDRESS пустой. Укажи адрес кошелька из настроек Polymarket.")

    for address in (wallet, get_proxy_wallet(wallet)):
        if address and address not in seen_addresses:
            candidate_addresses.append(address)
            seen_addresses.add(address)

    positions: Dict[str, Dict[str, float]] = {}
    stats = {
        "raw": 0,
        "weather": 0,
        "date_matches": 0,
        "size_kept": 0,
        "outcome_yes": 0,
        "final": 0,
    }
    skipped_small: List[str] = []

    for address in candidate_addresses:
        try:
            raw_positions = fetch_positions_for_user(address)
            print(f"Найдено raw-позиций через {address}: {len(raw_positions)}")
        except Exception as error:
            print(f"Ошибка получения позиций для {address}: {error}")
            continue

        for raw_position in raw_positions:
            stats["raw"] += 1

            if not is_weather_temperature_position(raw_position):
                continue
            stats["weather"] += 1

            if not is_target_date_position(raw_position):
                continue
            stats["date_matches"] += 1

            raw_size = get_effective_size(raw_position)
            if raw_size < MIN_POSITION_SIZE:
                skipped_small.append(
                    f"{raw_position.get('title', 'Unknown market')} | size={raw_size:.4f}"
                )
                continue
            stats["size_kept"] += 1

            position = normalize_position(raw_position)
            if not position:
                continue

            if position["outcome"] and position["outcome"] != "YES":
                continue
            stats["outcome_yes"] += 1

            positions[position["token_id"]] = position

    stats["final"] = len(positions)
    date_filter_note = " (date filter OFF)" if not TARGET_DATE_UTC else ""
    print(
        "SUMMARY | "
        f"raw={stats['raw']} | weather={stats['weather']} | target_date_matches={stats['date_matches']}{date_filter_note} | "
        f"min_size_kept>={MIN_POSITION_SIZE:.2f}:{stats['size_kept']} | yes={stats['outcome_yes']} | final_unique={stats['final']}"
    )

    if skipped_small:
        print(f"SKIPPED_BY_MIN_SIZE: {skipped_small[0]}")
        if len(skipped_small) > 1:
            print(f"SKIPPED_BY_MIN_SIZE_TOTAL: {len(skipped_small)}")

    return positions


def get_best_bid_price(token_id: str) -> Optional[float]:
    try:
        book = public_client.get_order_book(token_id)
        if not book.bids:
            return None
        return float(book.bids[0].price)
    except Exception as error:
        print(f"Не удалось получить стакан для {token_id}: {error}")
        return None


def get_best_ask_price(token_id: str) -> Optional[float]:
    try:
        book = public_client.get_order_book(token_id)
        if not book.asks:
            return None
        return float(book.asks[0].price)
    except Exception as error:
        print(f"Не удалось получить ask для {token_id}: {error}")
        return None


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
    outcomes = market.get("outcomes", [])
    extracted: List[dict] = []
    if isinstance(outcomes, list) and outcomes and isinstance(outcomes[0], dict):
        for outcome in outcomes:
            if str(outcome.get("outcome") or "").strip().upper() != "YES":
                continue
            token_id = str(outcome.get("token_id") or "").strip()
            if not token_id:
                continue
            extracted.append({"token_id": token_id, "price": safe_float(outcome.get("price"))})
        return extracted

    names = [str(v).strip().upper() for v in parse_list(market.get("outcomes"))]
    token_ids = [str(v).strip() for v in parse_list(market.get("tokenIds"))]
    if not token_ids:
        token_ids = [str(v).strip() for v in parse_list(market.get("clobTokenIds"))]
    prices = parse_list(market.get("outcomePrices"))
    for i, name in enumerate(names):
        if name != "YES":
            continue
        token_id = token_ids[i] if i < len(token_ids) else ""
        if not token_id:
            continue
        price = safe_float(prices[i] if i < len(prices) else 0)
        extracted.append({"token_id": token_id, "price": price})
    return extracted



def extract_city_from_market_title(title: str) -> str:
    text = (title or "").strip()
    if not text:
        return "unknown"
    match = re.search(r"\bin\s+(.+?)\s+be\b", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().lower()
    return text.lower()


def is_buy_question_phrase_match(market: dict) -> bool:
    if not BUY_REQUIRE_QUESTION_PHRASE:
        return True
    question_text = str(market.get("question") or market.get("title") or "").strip().lower()
    if not question_text:
        return False
    return BUY_REQUIRED_QUESTION_PHRASE in question_text


def clamp_buy_candidates(markets: List[dict]) -> List[dict]:
    if not markets:
        return []

    by_city: dict[str, List[dict]] = defaultdict(list)
    for market in markets:
        title = str(market.get("title") or "")
        city_key = extract_city_from_market_title(title)
        by_city[city_key].append(market)

    selected: List[dict] = []
    for city_markets in by_city.values():
        city_markets.sort(key=lambda item: abs(safe_float(item.get("price")) - 0.5))
        per_city_limit = max(1, BUY_MAX_CANDIDATES_PER_CITY)
        selected.extend(city_markets[:per_city_limit])

    selected.sort(key=lambda item: abs(safe_float(item.get("price")) - 0.5))
    if BUY_MAX_CANDIDATES_TOTAL > 0:
        limited = selected[:BUY_MAX_CANDIDATES_TOTAL]
    else:
        limited = selected
    total_limit_label = "unlimited" if BUY_MAX_CANDIDATES_TOTAL <= 0 else str(BUY_MAX_CANDIDATES_TOTAL)
    print(
        "BUY CANDIDATE CLAMP | "
        f"raw={len(markets)} -> by_city={len(selected)} -> capped={len(limited)} "
        f"| per_city={max(1, BUY_MAX_CANDIDATES_PER_CITY)} total={total_limit_label}"
    )
    return limited


def get_weather_markets_to_buy() -> List[dict]:
    markets: List[dict] = []
    seen_token_ids: Set[str] = set()
    offset = 0
    started = time.monotonic()
    scan_stats = {"batch": 0, "weather": 0, "phrase": 0, "date": 0, "yes": 0, "price_band": 0}
    while True:
        elapsed = time.monotonic() - started
        if elapsed > BUY_SCAN_MAX_DURATION_SEC:
            print(f"BUY SCAN TIMEBOX: достигли лимита {BUY_SCAN_MAX_DURATION_SEC}s, собрано={len(markets)}")
            break
        print(f"\rBUY SCAN PROGRESS | offset={offset} elapsed={elapsed:.1f}s", end="", flush=True)
        response = session.get(
            GAMMA_MARKETS_URL,
            params={"limit": 100, "offset": offset, "closed": "false", "archived": "false", "search": "temperature"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break
        scan_stats["batch"] += len(batch)
        for market in batch:
            if not is_weather_temperature_position(market):
                continue
            scan_stats["weather"] += 1
            if not is_buy_question_phrase_match(market):
                continue
            scan_stats["phrase"] += 1
            end_date_raw = str(market.get("endDate") or "").strip()
            if end_date_raw and end_date_raw[:10] not in target_dates():
                continue
            scan_stats["date"] += 1
            for outcome in extract_yes_outcomes(market):
                token_id = outcome["token_id"]
                if token_id in seen_token_ids:
                    continue
                seen_token_ids.add(token_id)
                outcome_price = safe_float(outcome["price"])
                scan_stats["yes"] += 1
                if not (BUY_MIN_PRICE <= outcome_price <= BUY_MAX_PRICE):
                    continue
                scan_stats["price_band"] += 1
                markets.append(
                    {
                        "title": market.get("question") or market.get("title") or "Unknown market",
                        "token_id": token_id,
                        "price": outcome_price,
                        "end_date": end_date_raw,
                    }
                )
                if len(markets) >= 500:
                    print()
                    return markets
        offset += 100
    print()
    print(
        "BUY SCAN DONE | "
        f"candidates={len(markets)} | scanned={scan_stats['batch']} | weather={scan_stats['weather']} | phrase_match={scan_stats['phrase']} | "
        f"date_match={scan_stats['date']} | yes_outcomes={scan_stats['yes']} | in_price_band={scan_stats['price_band']} | target_dates={sorted(target_dates())}"
    )
    return clamp_buy_candidates(markets)


def get_price_history_points(token_id: str, lookback_sec: int = 3600) -> List[tuple[int, float]]:
    now_ts = int(datetime.now(UTC).timestamp())
    start_ts = max(0, now_ts - lookback_sec)
    try:
        response = session.get(
            CLOB_PRICES_HISTORY_URL,
            params={"market": token_id, "startTs": start_ts, "endTs": now_ts, "fidelity": 5},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return []

    history = payload.get("history") if isinstance(payload, dict) else []
    points: List[tuple[int, float]] = []
    for point in history or []:
        try:
            ts = int(float(point.get("t")))
            price = float(point.get("p"))
        except Exception:
            continue
        if price > 0:
            points.append((ts, price))
    points.sort(key=lambda x: x[0])
    return points


def price_up_last_hour(token_id: str, current_price: float) -> bool:
    points = get_price_history_points(token_id, 3600)
    if not points:
        return False
    one_hour_ago = points[0][1]
    return current_price > one_hour_ago




def round_down(value: float, decimals: int = 2) -> float:
    factor = 10 ** decimals
    return math.floor(value * factor) / factor

def should_take_profit(entry_price: float, current_price: float) -> bool:
    return current_price >= entry_price * (1 + TAKE_PROFIT_PCT)


def should_sell(entry_price: float, current_price: float) -> bool:
    drawdown_price = entry_price * (1 - MAX_DRAWDOWN)
    absolute_stop_triggered = entry_price >= ABSOLUTE_SELL_PRICE and current_price <= ABSOLUTE_SELL_PRICE
    return should_take_profit(entry_price, current_price) or current_price <= drawdown_price or absolute_stop_triggered


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

        status = response.get("status") if isinstance(response, dict) else None
        order_id = response.get("orderID") if isinstance(response, dict) else None
        success = response.get("success") if isinstance(response, dict) else None

        print(
            f"SELL: token={token_id} reason={reason} bid={price:.2f} requested_size={size:.2f} sent_size={sell_size:.2f} "
            f"status={status} success={success} order_id={order_id}"
        )
        print(response)
        if isinstance(response, dict) and response.get("success") and reason == "stop_loss":
            register_stop_loss(token_id)
    except Exception as error:
        message = str(error)
        wallet = (WALLET_ADDRESS or "").strip().lower()
        resolved_funder = FUNDER_ADDRESS or get_proxy_wallet(wallet) or wallet
        if "invalid signature" in message.lower():
            print(
                "SELL ERROR: invalid signature. Проверь POLYMARKET_SIGNATURE_TYPE и POLYMARKET_FUNDER_ADDRESS "
                f"(сейчас type={SIGNATURE_TYPE}, funder={resolved_funder})."
            )
        elif "not enough balance / allowance" in message.lower():
            print(
                "SELL ERROR: not enough balance / allowance. "
                f"Попробуй меньше размер или проверь allowance; расчетный size={size:.4f}, buffer={SELL_SIZE_BUFFER}."
            )
        else:
            print(f"SELL ERROR: не удалось отправить ордер для {token_id}: {error}")


def buy(token_id: str, ask_price: float) -> None:
    if DRY_RUN:
        print(f"DRY RUN: BUY token={token_id} ask={ask_price:.2f} usd={BUY_SIZE_USD:.2f}")
        register_buy(token_id)
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
        buy_size = round_down(BUY_SIZE_USD / ask_price, 2)
        if buy_size <= 0:
            return
        order = OrderArgs(
            token_id=token_id,
            price=round(min(ask_price + 0.01, 0.99), 2),
            size=buy_size,
            side="BUY",
        )
        signed = client.create_order(order)
        response = client.post_order(signed, OrderType.GTC)
        print(
            f"BUY: token={token_id} ask={ask_price:.2f} usd={BUY_SIZE_USD:.2f} shares={buy_size:.2f} "
            f"success={response.get('success') if isinstance(response, dict) else None}"
        )
        if isinstance(response, dict) and response.get("success"):
            register_buy(token_id)
    except Exception as error:
        print(f"BUY ERROR: {token_id}: {error}")



# =========================
# MAIN LOOP
# =========================

while True:
    scan_started_at = datetime.now(UTC)
    print("\n=== WEATHER SELL SCAN ===")

    try:
        positions = get_positions()
        day_state = get_day_state()
        stopped_out_today = set(day_state.get("stopped_out_token_ids", []))
        if not positions:
            print("Погодных открытых YES-позиций не найдено.")

        for token_id, position in positions.items():
            entry = position["entry"]
            size = position["size"]
            api_price = position["cur_price"]
            bid_price = get_best_bid_price(token_id)
            effective_price = api_price if api_price > 0 else (bid_price or 0)
            stop_from_drawdown = entry * (1 - MAX_DRAWDOWN)
            take_profit_from_entry = entry * (1 + TAKE_PROFIT_PCT)

            if effective_price <= 0:
                print(
                    f"{position['title']} | {position['outcome']} | нет цены для проверки | "
                    f"asset={token_id}"
                )
                continue

            if should_sell(entry, effective_price):
                reason = []
                reason_code = "stop_loss"
                if should_take_profit(entry, effective_price):
                    reason.append(
                        f"take-profit +{TAKE_PROFIT_PCT:.0%} ({entry:.2f} -> {effective_price:.2f})"
                    )
                    reason_code = "take_profit"
                if effective_price <= stop_from_drawdown:
                    reason.append(
                        f"падение >=25% от входа ({entry:.2f} -> {effective_price:.2f})"
                    )
                if entry >= ABSOLUTE_SELL_PRICE and effective_price <= ABSOLUTE_SELL_PRICE:
                    reason.append(f"цена <= ${ABSOLUTE_SELL_PRICE:.2f} при входе >= ${ABSOLUTE_SELL_PRICE:.2f}")

                print(
                    f"{position['title']} | {position['outcome']} | size={size:.2f} cur={api_price:.2f} | SELL SIGNAL: {'; '.join(reason)}"
                )

                if bid_price is None:
                    print(
                        f"WAIT: позицию сейчас нельзя закрыть через стакан. "
                        f"curPrice={api_price:.2f}, size={size:.2f}, asset={token_id}"
                    )
                    continue

                sell(token_id, bid_price, size, reason=reason_code)
            else:
                print(
                    f"{position['title']} | {position['outcome']} | держим | "
                    f"size={size:.2f} entry={entry:.2f} cur={api_price:.2f} "
                    f"bid={bid_price if bid_price is not None else 'None'} end={position['end_date'][:10]} "
                    f"tp30={take_profit_from_entry:.2f} stop25={stop_from_drawdown:.2f}"
                )

        # BUY-блок с простыми правилами.
        markets = get_weather_markets_to_buy()
        existing_token_ids = set(positions.keys())
        print(
            f"BUY SESSION | found_markets={len(markets)} "
            f"buy_count_today={day_state.get('buy_count', 0)}/{MAX_NEW_BUYS_PER_DAY}"
        )
        buy_stats = {
            "seen": 0,
            "skipped_existing_or_stopped": 0,
            "skipped_no_effective_price": 0,
            "skipped_no_bid": 0,
            "skipped_spread": 0,
            "skipped_price_band": 0,
            "skipped_momentum": 0,
            "signals": 0,
        }
        for market in markets:
            buy_stats["seen"] += 1
            if day_state.get("buy_count", 0) >= MAX_NEW_BUYS_PER_DAY:
                print(f"BUY LIMIT REACHED: {MAX_NEW_BUYS_PER_DAY} на {utc_today()}")
                break

            token_id = market["token_id"]
            if token_id in existing_token_ids or token_id in stopped_out_today:
                buy_stats["skipped_existing_or_stopped"] += 1
                continue

            ask_price = get_best_ask_price(token_id)
            bid_price = get_best_bid_price(token_id)
            effective_buy_price = ask_price if ask_price is not None else market["price"]
            if effective_buy_price <= 0:
                buy_stats["skipped_no_effective_price"] += 1
                continue

            if bid_price is None or bid_price <= 0:
                buy_stats["skipped_no_bid"] += 1
                continue

            spread_cents = (effective_buy_price - bid_price) * 100
            if USE_SPREAD_FILTER and spread_cents > MAX_SPREAD_CENTS:
                buy_stats["skipped_spread"] += 1
                continue

            if not (BUY_MIN_PRICE <= effective_buy_price <= BUY_MAX_PRICE):
                buy_stats["skipped_price_band"] += 1
                continue

            if USE_HOURLY_MOMENTUM_FILTER and not price_up_last_hour(token_id, effective_buy_price):
                buy_stats["skipped_momentum"] += 1
                continue

            buy_stats["signals"] += 1
            print(
                f"{market['title']} | BUY SIGNAL | ask={effective_buy_price:.2f} "
                f"spread={spread_cents:.1f}c size_usd={BUY_SIZE_USD:.2f}"
            )
            buy(token_id, effective_buy_price)
            day_state = get_day_state()
            stopped_out_today = set(day_state.get("stopped_out_token_ids", []))

        print(
            "BUY FILTER SUMMARY | "
            f"seen={buy_stats['seen']} | signals={buy_stats['signals']} | "
            f"skip_existing_or_stopped={buy_stats['skipped_existing_or_stopped']} | "
            f"skip_no_effective_price={buy_stats['skipped_no_effective_price']} | "
            f"skip_no_bid={buy_stats['skipped_no_bid']} | skip_spread={buy_stats['skipped_spread']} (enabled={USE_SPREAD_FILTER}) | "
            f"skip_price_band={buy_stats['skipped_price_band']} | skip_momentum={buy_stats['skipped_momentum']}"
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
