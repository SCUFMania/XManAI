import math
import os
import time
from typing import Dict, List, Optional

import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType

# =========================
# НАСТРОЙКИ
# =========================

# Приватный ключ signer address.
PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")

# Адрес кошелька / signer address для поиска текущих позиций.
WALLET_ADDRESS = os.getenv("POLYMARKET_WALLET_ADDRESS", "")
FUNDER_ADDRESS = os.getenv("POLYMARKET_FUNDER_ADDRESS", "").strip().lower()
SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))

CHAIN_ID = 137
CHECK_INTERVAL = 30
DRY_RUN = True

# Для buy-бота дату лучше задавать вручную.
# Пример: 2026-03-23
TARGET_DATE_UTC = os.getenv("POLYMARKET_TARGET_DATE_UTC", "").strip()

# Покупаем только YES от 30 до 60 центов.
BUY_MIN_PRICE = 0.30
BUY_MAX_PRICE = 0.60

# На каждую новую позицию тратим только $1.
BUY_SIZE_USD = 1.00

# Фиксируем прибыль, если цена выросла на 30% от входа.
TAKE_PROFIT_PCT = 0.30

# Отсекаем совсем мелкие остатки, чтобы не шумели в логах.
MIN_POSITION_SIZE = float(os.getenv("POLYMARKET_MIN_POSITION_SIZE", "0.01"))

# Небольшой буфер для sell, чтобы не упираться в остатки/округления.
SELL_SIZE_BUFFER = 0.995

WEATHER_SLUG_PREFIX = "weather/temperature"
WEATHER_KEYWORDS = (
    "highest temperature",
    "temperature",
)

DATA_API_URL = "https://data-api.polymarket.com/positions"
PROFILE_API_URL = "https://gamma-api.polymarket.com/public-profile"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_API_URL = "https://clob.polymarket.com"
REQUEST_TIMEOUT = 20

# =========================

session = requests.Session()
public_client = ClobClient(CLOB_API_URL, chain_id=CHAIN_ID)
trading_client: Optional[ClobClient] = None
geoblock_cache: Optional[dict] = None


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
        raise ValueError(
            "PRIVATE_KEY должен быть hex-строкой длиной 64 символа."
        )

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


def is_weather_temperature_position(position: dict) -> bool:
    slug = (position.get("slug") or "").lower().lstrip("/")
    title = (position.get("title") or position.get("question") or "").lower()
    event_slug = (position.get("eventSlug") or "").lower().lstrip("/")

    return (
        slug.startswith(WEATHER_SLUG_PREFIX)
        or event_slug.startswith(WEATHER_SLUG_PREFIX)
        or any(keyword in title for keyword in WEATHER_KEYWORDS)
    )


def matches_target_date(payload: dict) -> bool:
    if not TARGET_DATE_UTC:
        raise ValueError(
            "Для buy-бота укажи POLYMARKET_TARGET_DATE_UTC вручную, например 2026-03-23."
        )

    end_date_raw = str(payload.get("endDate") or "").strip()
    if not end_date_raw:
        return False

    return end_date_raw[:10] == TARGET_DATE_UTC


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
            if not is_weather_temperature_position(raw_position):
                continue
            if not matches_target_date(raw_position):
                continue

            position = normalize_position(raw_position)
            if not position:
                continue

            if position["outcome"] != "YES":
                continue

            positions[position["token_id"]] = position

    return positions


def get_weather_markets_to_buy() -> List[dict]:
    if not TARGET_DATE_UTC:
        raise ValueError(
            "Для buy-бота укажи POLYMARKET_TARGET_DATE_UTC вручную, например 2026-03-23."
        )

    markets: List[dict] = []
    offset = 0

    while True:
        response = session.get(
            GAMMA_MARKETS_URL,
            params={
                "limit": 100,
                "offset": offset,
                "active": "true",
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        batch = response.json()
        if not batch:
            break

        for market in batch:
            if not is_weather_temperature_position(market):
                continue
            if not matches_target_date(market):
                continue

            for outcome in market.get("outcomes", []):
                if str(outcome.get("outcome") or "").strip().upper() != "YES":
                    continue

                token_id = str(outcome.get("token_id") or "").strip()
                if not token_id:
                    continue

                markets.append(
                    {
                        "title": market.get("question") or market.get("title") or "Unknown market",
                        "token_id": token_id,
                        "price": safe_float(outcome.get("price")),
                        "end_date": str(market.get("endDate") or ""),
                    }
                )

        offset += 100

    return markets


def get_best_bid_price(token_id: str) -> Optional[float]:
    try:
        book = public_client.get_order_book(token_id)
        return float(book.bids[0].price) if book.bids else None
    except Exception as error:
        print(f"Не удалось получить bid для {token_id}: {error}")
        return None


def get_best_ask_price(token_id: str) -> Optional[float]:
    try:
        book = public_client.get_order_book(token_id)
        return float(book.asks[0].price) if book.asks else None
    except Exception as error:
        print(f"Не удалось получить ask для {token_id}: {error}")
        return None


def should_take_profit(entry_price: float, current_price: float) -> bool:
    return current_price >= entry_price * (1 + TAKE_PROFIT_PCT)


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
    except Exception as error:
        print(f"BUY ERROR: не удалось отправить buy-ордер для {token_id}: {error}")


def sell(token_id: str, price: float, size: float) -> None:
    if DRY_RUN:
        print(f"DRY RUN: TAKE PROFIT SELL token={token_id} bid={price:.2f} size={size:.2f}")
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
            f"TAKE PROFIT SELL: token={token_id} bid={price:.2f} requested_size={size:.2f} sent_size={sell_size:.2f} "
            f"status={response.get('status') if isinstance(response, dict) else None} "
            f"success={response.get('success') if isinstance(response, dict) else None}"
        )
        print(response)
    except Exception as error:
        print(f"SELL ERROR: не удалось отправить take-profit sell для {token_id}: {error}")


# =========================
# MAIN LOOP
# =========================

while True:
    print("\n=== WEATHER BUY + TAKE PROFIT SCAN ===")

    try:
        positions = get_positions()
        print(f"Открытых YES-позиций на {TARGET_DATE_UTC}: {len(positions)}")

        # 1) Сначала фиксируем прибыль по уже открытым позициям.
        for token_id, position in positions.items():
            entry = position["entry"]
            size = position["size"]
            current_price = position["cur_price"] or 0
            bid_price = get_best_bid_price(token_id)
            effective_price = current_price if current_price > 0 else (bid_price or 0)
            take_profit_price = entry * (1 + TAKE_PROFIT_PCT)

            if effective_price <= 0:
                print(f"{position['title']} | нет цены для take-profit проверки")
                continue

            if should_take_profit(entry, effective_price):
                print(
                    f"{position['title']} | TAKE PROFIT SIGNAL | entry={entry:.2f} cur={effective_price:.2f} "
                    f"target={take_profit_price:.2f} size={size:.2f}"
                )
                if bid_price is None:
                    print(f"WAIT: нет bid для закрытия {token_id}")
                    continue
                sell(token_id, bid_price, size)
            else:
                print(
                    f"{position['title']} | держим | entry={entry:.2f} cur={effective_price:.2f} "
                    f"target={take_profit_price:.2f} size={size:.2f}"
                )

        # 2) Затем ищем новые покупки только среди рынков, которых у нас ещё нет.
        markets = get_weather_markets_to_buy()
        existing_token_ids = set(positions.keys())

        for market in markets:
            token_id = market["token_id"]
            if token_id in existing_token_ids:
                continue

            ask_price = get_best_ask_price(token_id)
            effective_buy_price = ask_price if ask_price is not None else market["price"]

            if effective_buy_price <= 0:
                continue

            if BUY_MIN_PRICE <= effective_buy_price <= BUY_MAX_PRICE:
                print(
                    f"{market['title']} | BUY SIGNAL | ask={effective_buy_price:.2f} "
                    f"budget=${BUY_SIZE_USD:.2f}"
                )
                buy(token_id, effective_buy_price)
            else:
                print(
                    f"{market['title']} | buy skip | ask={effective_buy_price:.2f} "
                    f"range={BUY_MIN_PRICE:.2f}-{BUY_MAX_PRICE:.2f}"
                )

    except Exception as error:
        print(f"Ошибка цикла мониторинга: {error}")

    time.sleep(CHECK_INTERVAL)
