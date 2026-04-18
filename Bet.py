#!/usr/bin/env python3
"""
Beethoven v1 bot for Polymarket BTC 5m market.

Strategy highlights implemented:
- Single fixed market (Bitcoin Up or Down - 5 Minutes).
- Entry price range filter: [0.67, 0.79].
- Re-entry: configurable attempts per side per 5m window (default 2).
- No new entries when time_to_close < 30s.
- TP arm at +13%, then trailing stop 2% from local peak.
- SL at -15% (bot-managed; place-limit hook is available in executor).
- Same-side cooldown for 10s after trailing-profit exits.
- Scan loop every 1 second.

Important:
- Spread filter is intentionally disabled for this revision.
- DRY_RUN defaults to true.
- Real order placement is intentionally conservative and must be wired via
  the executor methods for production use.
"""

from __future__ import annotations

import json
import logging
import os
import re
import signal
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Deque, Dict, Literal, Optional, Tuple
from urllib.parse import urlparse

import requests

Side = Literal["up", "down"]


# =========================
# CONFIG
# =========================
PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "2699b566bd147a62959ac383d5d9ae240310adf1ec50678777b6674a7679a95e")
WALLET_ADDRESS = os.getenv(
    "POLYMARKET_WALLET_ADDRESS", "0xe13E49fA8f1fCE54e10ffC1f9DD2f2329724935B"
).strip().lower()
FUNDER_ADDRESS = os.getenv(
    "POLYMARKET_FUNDER_ADDRESS", "0xe13E49fA8f1fCE54e10ffC1f9DD2f2329724935B"
).strip().lower()
SIGNATURE_TYPE = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "2"))

CHAIN_ID = int(os.getenv("POLYMARKET_CHAIN_ID", "137"))
DATA_API_URL = os.getenv("POLYMARKET_DATA_API_URL", "https://data-api.polymarket.com")
CLOB_API_URL = os.getenv("POLYMARKET_CLOB_API_URL", "https://clob.polymarket.com")
GAMMA_MARKETS_URL = os.getenv("POLYMARKET_GAMMA_MARKETS_URL", "https://gamma-api.polymarket.com/markets")
GAMMA_EVENTS_URL = os.getenv("POLYMARKET_GAMMA_EVENTS_URL", "https://gamma-api.polymarket.com/events")
REQUEST_TIMEOUT = int(os.getenv("POLYMARKET_REQUEST_TIMEOUT", "3"))
DISCOVERY_TIMEOUT = float(os.getenv("POLYMARKET_DISCOVERY_TIMEOUT", "1.5"))

# Requested by user: default DRY_RUN = true.
DRY_RUN = os.getenv("POLYMARKET_DRY_RUN", "0").strip().lower() in {"1", "true", "yes"}
CHECK_INTERVAL_SEC = float(os.getenv("POLYMARKET_CHECK_INTERVAL_SEC", "1"))
DISCOVERY_RETRY_SEC = float(os.getenv("POLYMARKET_DISCOVERY_RETRY_SEC", "1"))
MARKET_END_BUFFER_SEC = int(os.getenv("POLYMARKET_MARKET_END_BUFFER_SEC", "5"))
REJECT_CACHE_TTL_SEC = int(os.getenv("POLYMARKET_REJECT_CACHE_TTL_SEC", "120"))
BOOK_VALIDATION_CACHE_TTL_SEC = float(os.getenv("POLYMARKET_BOOK_VALIDATION_CACHE_TTL_SEC", "2.0"))
NO_ENTRY_LOG_EVERY_SEC = float(os.getenv("POLYMARKET_NO_ENTRY_LOG_EVERY_SEC", "3"))

# Strategy (v1)
POSITION_SIZE = float(os.getenv("POLYMARKET_POSITION_SIZE", "1"))
ENTRY_MIN_PRICE = float(os.getenv("POLYMARKET_ENTRY_MIN_PRICE", "0.67"))
ENTRY_MAX_PRICE = float(os.getenv("POLYMARKET_ENTRY_MAX_PRICE", "0.79"))
MARKET_WARMUP_SEC = float(os.getenv("POLYMARKET_MARKET_WARMUP_SEC", "8"))
MAX_ENTRY_ASK_PRICE = float(os.getenv("POLYMARKET_MAX_ENTRY_ASK_PRICE", "0.90"))
MIN_ENTRY_BID_PRICE = float(os.getenv("POLYMARKET_MIN_ENTRY_BID_PRICE", "0.02"))

TP_PCT = float(os.getenv("POLYMARKET_TP_PCT", "0.13"))
TRAILING_DROP_PCT = float(os.getenv("POLYMARKET_TRAILING_DROP_PCT", "0.02"))
SL_PCT = float(os.getenv("POLYMARKET_SL_PCT", "0.15"))
SL_GRACE_SEC = float(os.getenv("POLYMARKET_SL_GRACE_SEC", "0.5"))
SL_EARLY_BUFFER_PCT = float(os.getenv("POLYMARKET_SL_EARLY_BUFFER_PCT", "0.01"))
FIRST_ENTRY_MIN_MOMENTUM_5S = float(os.getenv("POLYMARKET_FIRST_ENTRY_MIN_MOMENTUM_5S", "0.05"))
REENTRY_MIN_MOMENTUM_5S = float(os.getenv("POLYMARKET_REENTRY_MIN_MOMENTUM_5S", "0.05"))
OPEN_POSITION_CHECK_INTERVAL_SEC = float(os.getenv("POLYMARKET_OPEN_POSITION_CHECK_INTERVAL_SEC", "0.10"))
SUMMARY_LOG_EVERY_SEC = float(os.getenv("POLYMARKET_SUMMARY_LOG_EVERY_SEC", "30"))

MAX_ATTEMPTS_PER_SIDE_PER_WINDOW = int(
    os.getenv("POLYMARKET_MAX_ATTEMPTS_PER_SIDE_PER_WINDOW", "2")
)
NO_NEW_ENTRY_IF_TTC_LT_SEC = int(os.getenv("POLYMARKET_NO_ENTRY_IF_TTC_LT_SEC", "30"))
SAME_SIDE_COOLDOWN_SEC = int(os.getenv("POLYMARKET_SAME_SIDE_COOLDOWN_SEC", "10"))

BTC_5M_QUERY = os.getenv("POLYMARKET_BTC_5M_QUERY", "Bitcoin Up or Down - 5 Minutes")
# Optional exact slug override; rolling discovery does not rely on exact slug.
BTC_5M_SLUG = os.getenv("POLYMARKET_BTC_5M_SLUG", "").strip().lower()
POLYMARKET_EVENT_URL = os.getenv(
    "POLYMARKET_EVENT_URL", "https://polymarket.com/event/btc-updown-5m-"
).strip()
UP_TOKEN_ID = os.getenv("POLYMARKET_UP_TOKEN_ID", "").strip()
DOWN_TOKEN_ID = os.getenv("POLYMARKET_DOWN_TOKEN_ID", "").strip()

STATE_FILE = Path(os.getenv("POLYMARKET_STATE_FILE", "beet_v1_state.json"))
TRADES_LOG_FILE = Path(os.getenv("POLYMARKET_TRADES_LOG_FILE", "beet_v1_trades.json"))


# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=os.getenv("POLYMARKET_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("beethoven_v1")


# =========================
# MODELS
# =========================
@dataclass
class PositionState:
    window_id: int
    side: Side
    entry_price: float
    entry_time: float
    size: float
    trailing_armed: bool = False
    peak_price_since_trailing: Optional[float] = None
    max_favorable_return: float = 0.0
    max_adverse_return: float = 0.0


@dataclass
class WindowState:
    window_id: int
    active: bool = False
    current_position: Optional[PositionState] = None
    attempt_count_per_side: Dict[Side, int] = field(
        default_factory=lambda: {"up": 0, "down": 0}
    )
    same_side_cooldown_until: Dict[Side, float] = field(
        default_factory=lambda: {"up": 0.0, "down": 0.0}
    )
    last_exit_reason: Optional[str] = None
    last_exit_pnl: Optional[float] = None


@dataclass
class SessionStats:
    opened_total: int = 0
    closed_total: int = 0
    closed_tp_total: int = 0
    closed_sl_total: int = 0


@dataclass
class MarketSnapshot:
    timestamp: float
    window_id: int
    time_to_close_sec: float
    prices: Dict[Side, float]  # mark/reference prices for momentum & PnL tracking
    buy_prices: Dict[Side, float]  # executable buy prices (best ask)
    bids: Dict[Side, float]
    asks: Dict[Side, float]
    ref_prices: Dict[Side, float]
    spreads: Dict[Side, float]
    top_bid_sizes: Dict[Side, float]
    top_ask_sizes: Dict[Side, float]


@dataclass
class SideEvaluation:
    side: Side
    eligible: bool
    reason: str
    momentum_ok: bool
    momentum_score: float
    buy_price: float
    ref_price: float


class DiscoveryError(RuntimeError):
    pass


class InvalidMarketCandidate(DiscoveryError):
    pass


class SnapshotError(RuntimeError):
    pass


class OrderbookUnavailable(SnapshotError):
    pass


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> Dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Could not load state file (%s): %s", self.path, exc)
            return {}

    def save(self, payload: Dict) -> None:
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class TradeLogger:
    def __init__(self, path: Path) -> None:
        self.path = path

    def log(self, event: Dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")


# =========================
# API + EXECUTION ADAPTERS
# =========================
class PolymarketDataClient:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.rejected_candidates: Dict[str, float] = {}
        self.token_book_cache: Dict[str, Tuple[float, Optional[Dict]]] = {}
        self.last_book_parse_log_at = 0.0

    def _get_json(self, url: str, params: Optional[Dict] = None, timeout: Optional[float] = None) -> object:
        r = self.s.get(url, params=params or {}, timeout=timeout or REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def _discover_from_event_page(self, event_url: str) -> Optional[Dict]:
        try:
            r = self.s.get(event_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            r.raise_for_status()
        except Exception:
            return None

        html = r.text
        final_slug = ""
        try:
            path = urlparse(r.url).path
            if "/event/" in path:
                final_slug = path.split("/event/", 1)[1].strip("/")
        except Exception:
            pass

        # Parse token IDs and outcomes from page JSON payload.
        ids_match = re.search(r'"clobTokenIds"\s*:\s*(\[[^\]]+\])', html)
        outcomes_match = re.search(r'"outcomes"\s*:\s*(\[[^\]]+\])', html)
        question_match = re.search(r'"question"\s*:\s*"([^"]+)"', html)
        if not ids_match:
            return None
        try:
            clob_ids = json.loads(ids_match.group(1))
            outcomes = json.loads(outcomes_match.group(1)) if outcomes_match else ["Up", "Down"]
        except Exception:
            return None
        if not isinstance(clob_ids, list) or len(clob_ids) < 2:
            return None

        return {
            "id": "event-page",
            "slug": final_slug or BTC_5M_SLUG,
            "question": question_match.group(1) if question_match else BTC_5M_QUERY,
            "outcomes": outcomes,
            "clobTokenIds": clob_ids,
        }

    def _discover_from_clob_markets(self) -> Optional[Dict]:
        """
        Fallback: crawl CLOB markets endpoint and try to find BTC 5m market
        by question/slug, then extract two token IDs.
        """
        cursor = ""
        for _ in range(20):
            params = {"limit": 200}
            if cursor:
                params["next_cursor"] = cursor
            try:
                payload = self._get_json(f"{CLOB_API_URL}/markets", params=params)
            except Exception:
                return None

            if isinstance(payload, dict):
                markets = payload.get("data") or payload.get("markets") or []
                cursor = str(payload.get("next_cursor") or "")
            elif isinstance(payload, list):
                markets = payload
                cursor = ""
            else:
                markets = []
                cursor = ""

            for m in markets:
                slug = str(m.get("slug") or "").lower()
                question = str(m.get("question") or m.get("title") or "").lower()
                looks_like_target = (
                    ("bitcoin up or down" in question and "5 minute" in question)
                    or ("btc" in slug and "5m" in slug and ("updown" in slug or "up-or-down" in slug))
                )
                if not looks_like_target:
                    continue

                clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or m.get("token_ids")
                outcomes = m.get("outcomes") or ["Up", "Down"]
                if isinstance(clob_ids, str):
                    try:
                        clob_ids = json.loads(clob_ids)
                    except Exception:
                        clob_ids = None

                if (not clob_ids or len(clob_ids) < 2) and isinstance(m.get("tokens"), list):
                    token_ids = []
                    token_outcomes = []
                    for t in m.get("tokens", []):
                        tid = t.get("token_id") or t.get("tokenId")
                        if tid:
                            token_ids.append(str(tid))
                            token_outcomes.append(str(t.get("outcome") or t.get("name") or ""))
                    if len(token_ids) >= 2:
                        clob_ids = token_ids
                        outcomes = token_outcomes if token_outcomes else outcomes

                if clob_ids and len(clob_ids) >= 2:
                    return {
                        "id": m.get("id") or "clob-market",
                        "slug": m.get("slug") or BTC_5M_SLUG,
                        "question": m.get("question") or m.get("title") or BTC_5M_QUERY,
                        "outcomes": outcomes,
                        "clobTokenIds": clob_ids,
                    }

            if not cursor or cursor in {"0", "null", "None"}:
                break
        return None

    @staticmethod
    def _normalize_market(m: Dict) -> Optional[Dict]:
        outcomes = m.get("outcomes")
        clob_ids = m.get("clobTokenIds") or m.get("clob_token_ids") or m.get("token_ids")
        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                outcomes = []
        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = []
        if not isinstance(outcomes, list) or not isinstance(clob_ids, list):
            return None
        if len(outcomes) < 2 or len(clob_ids) < 2:
            return None
        out = dict(m)
        out["outcomes"] = outcomes[:2]
        out["clobTokenIds"] = [str(x) for x in clob_ids[:2]]
        return out

    @staticmethod
    def _candidate_key(market: Dict) -> str:
        return str(market.get("id") or market.get("slug") or "unknown")

    def mark_candidate_rejected(self, market: Dict, reason: str) -> None:
        key = self._candidate_key(market)
        self.rejected_candidates[key] = time.time() + REJECT_CACHE_TTL_SEC
        logger.warning(
            "Rejected market key=%s id=%s slug=%s question=%s token_ids=%s reason=%s",
            key,
            market.get("id"),
            market.get("slug"),
            market.get("question") or market.get("title"),
            market.get("clobTokenIds"),
            reason,
        )

    def _is_temporarily_rejected(self, market: Dict) -> bool:
        key = self._candidate_key(market)
        until = self.rejected_candidates.get(key, 0)
        return until > time.time()

    def _get_book_cached(self, token_id: str, allow_stale: bool = False) -> Optional[Dict]:
        cached = self.token_book_cache.get(token_id)
        if not cached:
            return None
        ts, payload = cached
        if allow_stale or (time.time() - ts) <= BOOK_VALIDATION_CACHE_TTL_SEC:
            return payload
        return None

    def _fetch_book(self, token_id: str, timeout: Optional[float] = None) -> Tuple[int, Optional[Dict]]:
        r = self.s.get(
            f"{CLOB_API_URL}/book",
            params={"token_id": token_id},
            timeout=timeout or REQUEST_TIMEOUT,
        )
        if r.status_code != 200:
            return r.status_code, None
        try:
            payload = r.json()
        except Exception:
            return 500, None
        self.token_book_cache[token_id] = (time.time(), payload)
        return 200, payload

    @staticmethod
    def _parse_level_price(level: Dict) -> Optional[float]:
        raw = level.get("price")
        try:
            px = float(raw)
            return px if px > 0 else None
        except Exception:
            return None

    @staticmethod
    def _parse_level_size(level: Dict) -> float:
        raw = level.get("size") or level.get("amount") or level.get("quantity") or 0.0
        try:
            return max(0.0, float(raw))
        except Exception:
            return 0.0

    def _extract_best_bid_level(self, levels: list[Dict]) -> Optional[Dict]:
        best_level = None
        best_price = -1.0
        for lvl in levels:
            px = self._parse_level_price(lvl)
            if px is None:
                continue
            if px > best_price:
                best_price = px
                best_level = lvl
        return best_level

    def _extract_best_ask_level(self, levels: list[Dict]) -> Optional[Dict]:
        best_level = None
        best_price = float("inf")
        for lvl in levels:
            px = self._parse_level_price(lvl)
            if px is None:
                continue
            if px < best_price:
                best_price = px
                best_level = lvl
        return best_level

    def _probe_token_book(self, token_id: str) -> Tuple[bool, str]:
        if self._get_book_cached(token_id) is not None:
            return True, "ok"
        status, _ = self._fetch_book(token_id, timeout=DISCOVERY_TIMEOUT)
        if status == 200:
            return True, "ok"
        return False, f"token book status={status}"

    def _validate_market_candidate(self, market: Dict, probe_books: bool = False) -> Tuple[bool, str]:
        norm = self._normalize_market(market)
        if not norm:
            return False, "normalize_failed"
        slug = str(norm.get("slug") or "").lower()
        question = str(norm.get("question") or norm.get("title") or "").lower()
        end_ts = self._extract_end_ts(norm)
        if "active" in norm and not bool(norm.get("active")):
            return False, "active_false"
        if "closed" in norm and bool(norm.get("closed")):
            return False, "closed_true"
        if "enableOrderBook" in norm and not bool(norm.get("enableOrderBook")):
            return False, "enableOrderBook_false"
        if end_ts <= time.time() + MARKET_END_BUFFER_SEC:
            return False, f"expired_or_near_expiry_end_ts={end_ts}"
        if not (
            slug.startswith("btc-updown-5m-")
            or ("bitcoin up or down" in question and "5 minute" in question)
        ):
            return False, "not_btc_5m"
        outcomes = [str(x).strip().lower() for x in (norm.get("outcomes") or [])]
        if len(outcomes) != 2 or set(outcomes) not in ({"up", "down"}, {"yes", "no"}):
            return False, f"malformed_outcomes={outcomes}"
        token_ids = norm.get("clobTokenIds") or []
        if len(token_ids) != 2 or not all(str(t).strip() for t in token_ids):
            return False, "missing_token_ids"
        if probe_books:
            for token in token_ids:
                ok, reason = self._probe_token_book(str(token))
                if not ok:
                    return False, f"{reason} token={token}"
        return True, "ok"

    def _market_tokens(self, market: Dict) -> list[str]:
        norm = self._normalize_market(market)
        if not norm:
            return []
        return [str(t) for t in (norm.get("clobTokenIds") or [])[:2]]

    def _ensure_market_tradable(self, market: Dict, stage: str) -> Tuple[bool, str]:
        tokens = self._market_tokens(market)
        if len(tokens) != 2:
            return False, f"{stage}: missing_token_ids"
        for token in tokens:
            ok, reason = self._probe_token_book(token)
            if not ok:
                return False, f"{stage}: {reason} token={token}"
        return True, "ok"

    def _first_valid_candidate(self, candidates: list[Dict], stage: str) -> Optional[Dict]:
        # Prefer earliest still-active market. Probe books only after cheap filters.
        structurally_valid: list[Dict] = []
        for raw in candidates:
            norm = self._normalize_market(raw)
            if not norm:
                self.mark_candidate_rejected(raw, f"{stage}: normalize_failed")
                continue
            if self._is_temporarily_rejected(norm):
                logger.debug("Discovery stage=%s skip recent reject id=%s slug=%s", stage, norm.get("id"), norm.get("slug"))
                continue
            ok, reason = self._validate_market_candidate(norm, probe_books=False)
            if not ok:
                self.mark_candidate_rejected(norm, f"{stage}: {reason}")
                continue
            structurally_valid.append(norm)
        structurally_valid.sort(key=self._extract_end_ts)
        for norm in structurally_valid:
            tradable, treason = self._ensure_market_tradable(norm, stage=stage)
            if tradable:
                return norm
            self.mark_candidate_rejected(norm, treason)
        return None

    def discover_market(self, query: str, strict_slug: str) -> Dict:
        if UP_TOKEN_ID and DOWN_TOKEN_ID:
            for token in (UP_TOKEN_ID, DOWN_TOKEN_ID):
                ok, reason = self._probe_token_book(token)
                if not ok:
                    raise InvalidMarketCandidate(f"env token invalid: {reason}")
            logger.info("Using explicit token IDs from env, market discovery bypassed.")
            return {
                "id": "env-configured",
                "slug": strict_slug,
                "question": query,
                "outcomes": ["Up", "Down"],
                "clobTokenIds": [UP_TOKEN_ID, DOWN_TOKEN_ID],
            }
        # 1) exact rolling slug candidates first (fast path)
        slug_candidates = self._build_slug_candidates(strict_slug or "btc-updown-5m-")
        exact_candidates: list[Dict] = []
        for slug in slug_candidates:
            try:
                payload = self._get_json(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=DISCOVERY_TIMEOUT)
            except Exception:
                continue
            events = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
            for ev in events:
                exact_candidates.extend(ev.get("markets", []) or [])
        selected = self._first_valid_candidate(exact_candidates, stage="exact_slug")
        if selected:
            logger.info("Discovery stage=exact_slug selected id=%s slug=%s", selected.get("id"), selected.get("slug"))
            return selected

        # 2) event page parse fallback
        page_market = self._discover_from_event_page(POLYMARKET_EVENT_URL)
        if page_market:
            selected = self._first_valid_candidate([page_market], stage="event_page")
            if selected:
                logger.info("Discovery stage=event_page selected id=%s slug=%s", selected.get("id"), selected.get("slug"))
                return selected

        # 3) gamma scan last fallback (heavier)
        for offset in range(0, 3000, 100):
            try:
                page = self._get_json(
                    GAMMA_MARKETS_URL,
                    params={"limit": 100, "offset": offset},
                    timeout=DISCOVERY_TIMEOUT,
                )
            except Exception as exc:
                logger.debug("Discovery stage=gamma_scan page_error offset=%s err=%s", offset, exc)
                break
            markets = page if isinstance(page, list) else []
            if not markets:
                break
            candidates = []
            for m in markets:
                slug = str(m.get("slug") or "").lower()
                question = str(m.get("question") or m.get("title") or "").lower()
                if slug.startswith("btc-updown-5m-") or ("bitcoin up or down" in question and "5 minute" in question):
                    candidates.append(m)
            selected = self._first_valid_candidate(candidates, stage="gamma_scan")
            if selected:
                logger.info("Discovery stage=gamma_scan selected id=%s slug=%s", selected.get("id"), selected.get("slug"))
                return selected

        # 4) robust CLOB market crawl last-resort fallback
        clob_market = self._discover_from_clob_markets()
        if clob_market:
            selected = self._first_valid_candidate([clob_market], stage="clob_scan")
            if selected:
                logger.info("Discovery stage=clob_scan selected id=%s slug=%s", selected.get("id"), selected.get("slug"))
                return selected

        raise DiscoveryError("No valid BTC 5m market candidate found (all candidates rejected or unavailable).")

    @staticmethod
    def _build_slug_candidates(strict_slug: str) -> list[str]:
        """
        BTC 5m slugs are rotating and usually include epoch suffix:
        btc-updown-5m-1776249300
        Build nearby window candidates automatically.
        """
        base = (strict_slug or "btc-updown-5m-").strip().lower()
        cands: list[str] = [base]
        if base.endswith("-"):
            cands.append(base.rstrip("-"))

        tail = base.rstrip("-").split("-")[-1]
        if tail.isdigit():
            return list(dict.fromkeys(cands))

        prefix = base.rstrip("-")
        now = int(time.time())
        win = now - (now % 300)
        for delta in (-600, -300, 0, 300, 600):
            cands.append(f"{prefix}-{win + delta}")
        return list(dict.fromkeys(cands))

    @staticmethod
    def _extract_token_ids(market: Dict) -> Dict[Side, str]:
        # Tries common market schemas from Gamma.
        outcomes = market.get("outcomes")
        clob_ids = market.get("clobTokenIds")

        if isinstance(outcomes, str):
            try:
                outcomes = json.loads(outcomes)
            except Exception:
                outcomes = []

        if isinstance(clob_ids, str):
            try:
                clob_ids = json.loads(clob_ids)
            except Exception:
                clob_ids = []

        mapping: Dict[str, str] = {}
        if isinstance(outcomes, list) and isinstance(clob_ids, list):
            for i, outcome in enumerate(outcomes):
                if i < len(clob_ids):
                    mapping[str(outcome).strip().lower()] = str(clob_ids[i])

        up = mapping.get("up") or mapping.get("yes")
        down = mapping.get("down") or mapping.get("no")
        if not up or not down:
            raise InvalidMarketCandidate("Could not resolve up/down token IDs from market payload")
        return {"up": up, "down": down}

    def fetch_snapshot(self, market: Dict) -> MarketSnapshot:
        token_ids = self._extract_token_ids(market)
        now = time.time()

        bids: Dict[Side, float] = {}
        asks: Dict[Side, float] = {}
        ref_prices: Dict[Side, float] = {}
        buy_prices: Dict[Side, float] = {}
        spreads: Dict[Side, float] = {}
        top_bid_sizes: Dict[Side, float] = {}
        top_ask_sizes: Dict[Side, float] = {}
        for side, token in token_ids.items():
            # Reuse short-lived cache warmed by discovery to avoid duplicate /book hits.
            book = self._get_book_cached(token)
            if book is None:
                status, book = self._fetch_book(token, timeout=REQUEST_TIMEOUT)
                if status in (400, 404):
                    raise OrderbookUnavailable(
                        f"token book unavailable status={status} side={side} token={token}"
                    )
                if status != 200 or book is None:
                    raise SnapshotError(f"book fetch failed side={side} token={token} status={status}")
            bid_levels = book.get("bids") or []
            asks_list = book.get("asks") or []
            best_bid_level = self._extract_best_bid_level(bid_levels)
            best_ask_level = self._extract_best_ask_level(asks_list)
            best_bid = self._parse_level_price(best_bid_level or {}) or 0.0
            best_ask = self._parse_level_price(best_ask_level or {}) or 0.0
            bid_size = self._parse_level_size(best_bid_level or {})
            ask_size = self._parse_level_size(best_ask_level or {})
            if time.time() - self.last_book_parse_log_at >= 30:
                self.last_book_parse_log_at = time.time()
                raw_bid_prices = [
                    self._parse_level_price(lvl) for lvl in bid_levels[:5] if self._parse_level_price(lvl) is not None
                ]
                raw_ask_prices = [
                    self._parse_level_price(lvl) for lvl in asks_list[:5] if self._parse_level_price(lvl) is not None
                ]
                logger.info(
                    "book-parse side=%s raw_bids=%s raw_asks=%s best_bid=%.4f best_ask=%.4f",
                    side,
                    raw_bid_prices,
                    raw_ask_prices,
                    best_bid,
                    best_ask,
                )
            if best_bid <= 0 and best_ask <= 0:
                raise SnapshotError(f"No prices for side={side}, token={token}")
            bids[side] = best_bid
            asks[side] = best_ask
            buy_prices[side] = best_ask if best_ask > 0 else best_bid
            # Spread is kept for diagnostics/logging only (not an entry decision gate).
            spreads[side] = max(0.0, best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 1.0
            top_bid_sizes[side] = bid_size
            top_ask_sizes[side] = ask_size

            # Reference price priority:
            # 1) displayed/last-trade-like fields from payload
            # 2) executable buy price fallback
            raw_last = (
                book.get("last_trade_price")
                or book.get("lastPrice")
                or book.get("mark_price")
                or book.get("mid")
            )
            try:
                ref = float(raw_last) if raw_last is not None else 0.0
            except Exception:
                ref = 0.0
            if ref <= 0:
                ref = buy_prices[side]
            ref_prices[side] = ref

        # Canonical binary complement: if both refs exist, derive DOWN from UP.
        up_ref = ref_prices.get("up", 0.0)
        if 0 < up_ref < 1:
            ref_prices["down"] = max(0.0, min(1.0, 1.0 - up_ref))

        end_ts = self._extract_end_ts(market)
        if end_ts <= now + MARKET_END_BUFFER_SEC:
            raise InvalidMarketCandidate(f"market end_ts too close/expired end_ts={end_ts}")
        ttc = max(0.0, end_ts - now)
        window_id = int(end_ts // 300)

        return MarketSnapshot(
            timestamp=now,
            window_id=window_id,
            time_to_close_sec=ttc,
            prices=ref_prices,
            buy_prices=buy_prices,
            bids=bids,
            asks=asks,
            ref_prices=ref_prices,
            spreads=spreads,
            top_bid_sizes=top_bid_sizes,
            top_ask_sizes=top_ask_sizes,
        )

    @staticmethod
    def _extract_end_ts(market: Dict) -> float:
        candidates = [market.get("endDate"), market.get("endDateIso"), market.get("endTime")]
        for c in candidates:
            if not c:
                continue
            if isinstance(c, (int, float)):
                return float(c)
            if isinstance(c, str):
                try:
                    dt = datetime.fromisoformat(c.replace("Z", "+00:00"))
                    return dt.timestamp()
                except Exception:
                    pass
        # Fallback to next 5m boundary.
        now = time.time()
        return (int(now // 300) + 1) * 300


class TradeExecutor:
    def __init__(self, dry_run: bool) -> None:
        self.dry_run = dry_run

    def open_position(self, side: Side, size: float, entry_price: float) -> str:
        order_id = f"dry-open-{int(time.time()*1000)}"
        if self.dry_run:
            logger.info("[DRY-RUN] OPEN %s size=%.4f price=%.4f", side, size, entry_price)
            return order_id
        # TODO(live-1): Implement signed order placement for open_position via Polymarket CLOB API.
        raise NotImplementedError("Live order placement is not wired in this version")

    def place_protective_sl(self, side: Side, size: float, sl_price: float) -> str:
        order_id = f"dry-sl-{int(time.time()*1000)}"
        if self.dry_run:
            logger.info("[DRY-RUN] PLACE SL %s size=%.4f sl_price=%.4f", side, size, sl_price)
            return order_id
        # TODO(live-2): Implement protective SL order placement for place_protective_sl.
        raise NotImplementedError("Live SL placement is not wired in this version")

    def close_position(self, side: Side, size: float, reason: str) -> str:
        order_id = f"dry-close-{int(time.time()*1000)}"
        if self.dry_run:
            logger.info("[DRY-RUN] CLOSE %s size=%.4f reason=%s", side, size, reason)
            return order_id
        # TODO(live-3): Implement live close_position order placement.
        raise NotImplementedError("Live close placement is not wired in this version")


# =========================
# CORE STRATEGY ENGINE
# =========================
class BeethovenV1Bot:
    def __init__(self) -> None:
        self.data = PolymarketDataClient()
        self.exec = TradeExecutor(DRY_RUN)
        self.store = StateStore(STATE_FILE)
        self.trade_log = TradeLogger(TRADES_LOG_FILE)

        self.market: Optional[Dict] = None
        self.window_state: Optional[WindowState] = None
        self.session_stats = SessionStats()
        self.price_history: Dict[Side, Deque[Tuple[float, float]]] = {
            "up": deque(maxlen=30),
            "down": deque(maxlen=30),
        }
        self.stop_requested = False
        self.next_discovery_at = 0.0
        self.discovery_fail_count = 0
        self.last_no_entry_info_at = 0.0
        self.market_key: Optional[str] = None
        self.market_changed_at = 0.0
        self.last_summary_log_at = 0.0
        self.session_trade_stats: list[Dict] = []

        self._load_state()

    def _load_state(self) -> None:
        payload = self.store.load()
        if not payload:
            return
        ws = payload.get("window_state")
        if not ws:
            self.window_state = WindowState(window_id=self._current_window_id())
        else:
            pos = ws.get("current_position")
            current_position = PositionState(**pos) if pos else None
            self.window_state = WindowState(
                window_id=ws["window_id"],
                active=ws["active"],
                current_position=current_position,
                attempt_count_per_side=ws.get("attempt_count_per_side", {"up": 0, "down": 0}),
                same_side_cooldown_until=ws.get("same_side_cooldown_until", {"up": 0.0, "down": 0.0}),
                last_exit_reason=ws.get("last_exit_reason"),
                last_exit_pnl=ws.get("last_exit_pnl"),
            )
        self.session_stats = SessionStats(**payload.get("session_stats", {}))

    def _save_state(self) -> None:
        ws_dict = None
        if self.window_state:
            ws_dict = asdict(self.window_state)
        self.store.save(
            {
                "window_state": ws_dict,
                "session_stats": asdict(self.session_stats),
                "updated_at": time.time(),
            }
        )

    def _current_window_id(self) -> int:
        return int(time.time() // 300)

    def _ensure_window_state(self, window_id: int) -> None:
        # Keep unresolved/open position alive across window rollover (hold until resolution).
        if self.window_state and self.window_state.active and self.window_state.current_position:
            return
        if self.window_state is None or self.window_state.window_id != window_id:
            if self.window_state and self.window_state.window_id != window_id:
                logger.info("Window rollover reset: from_window=%s to_window=%s", self.window_state.window_id, window_id)
            self.window_state = WindowState(window_id=window_id)

    def _market_should_rediscover(self, snap: MarketSnapshot) -> bool:
        # If no open position, proactively switch market near expiry.
        if self.window_state and self.window_state.active and self.window_state.current_position:
            return False
        return snap.time_to_close_sec <= NO_NEW_ENTRY_IF_TTC_LT_SEC

    @staticmethod
    def _market_identity(market: Dict) -> str:
        return str(market.get("id") or market.get("slug") or "unknown")

    def _on_market_changed(self, market: Dict) -> None:
        new_key = self._market_identity(market)
        if self.market_key == new_key:
            return
        self.market_key = new_key
        self.market_changed_at = time.time()
        self.price_history = {"up": deque(maxlen=30), "down": deque(maxlen=30)}
        logger.info("Reset price history for new market id=%s slug=%s", market.get("id"), market.get("slug"))

    def _info_no_entry(
        self,
        snap: MarketSnapshot,
        side_signal: Optional[Side],
        reason: str,
    ) -> None:
        now = time.time()
        if now - self.last_no_entry_info_at < NO_ENTRY_LOG_EVERY_SEC:
            return
        self.last_no_entry_info_at = now
        up_now, up_5, up_10 = self._momentum_debug_values("up", snap.timestamp)
        dn_now, dn_5, dn_10 = self._momentum_debug_values("down", snap.timestamp)
        up_d5 = None if up_5 is None or up_now is None else up_now - up_5
        up_d10 = None if up_10 is None or up_now is None else up_now - up_10
        dn_d5 = None if dn_5 is None or dn_now is None else dn_now - dn_5
        dn_d10 = None if dn_10 is None or dn_now is None else dn_now - dn_10
        logger.info(
            "no-entry up_bid=%.4f up_ask=%.4f down_bid=%.4f down_ask=%.4f "
            "up_ref=%.4f down_ref=%.4f "
            "up_top_bid_size=%.4f up_top_ask_size=%.4f down_top_bid_size=%.4f down_top_ask_size=%.4f "
            "ttc=%.1fs side=%s reason=%s "
            "delta_up_5=%s delta_up_10=%s delta_down_5=%s delta_down_10=%s",
            snap.bids["up"],
            snap.asks["up"],
            snap.bids["down"],
            snap.asks["down"],
            snap.ref_prices["up"],
            snap.ref_prices["down"],
            snap.top_bid_sizes["up"],
            snap.top_ask_sizes["up"],
            snap.top_bid_sizes["down"],
            snap.top_ask_sizes["down"],
            snap.time_to_close_sec,
            side_signal or "none",
            reason,
            f"{up_d5:+.4f}" if up_d5 is not None else "None",
            f"{up_d10:+.4f}" if up_d10 is not None else "None",
            f"{dn_d5:+.4f}" if dn_d5 is not None else "None",
            f"{dn_d10:+.4f}" if dn_d10 is not None else "None",
        )

    @staticmethod
    def _price_at_or_before(hist: Deque[Tuple[float, float]], ts: float) -> Optional[float]:
        candidate = None
        for t, p in hist:
            if t <= ts:
                candidate = p
        return candidate

    def _momentum_ok_5s(self, side: Side, now: float, current_price: float) -> bool:
        hist = self.price_history[side]
        p5 = self._price_at_or_before(hist, now - 5)
        if p5 is None:
            return False
        return current_price > p5

    def _momentum_debug_values(self, side: Side, now: float) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        hist = self.price_history[side]
        p_now = hist[-1][1] if hist else None
        p5 = self._price_at_or_before(hist, now - 5)
        p10 = self._price_at_or_before(hist, now - 10)
        return p_now, p5, p10

    def _momentum_score(self, side: Side, now: float, current_ref_price: float, attempts: int) -> float:
        hist = self.price_history[side]
        p5 = self._price_at_or_before(hist, now - 5)
        if p5 is None or p5 <= 0:
            return float("-inf")
        score5 = (current_ref_price - p5) / p5
        return score5

    def _momentum_passes_threshold(self, attempts: int, score5: float) -> bool:
        threshold = FIRST_ENTRY_MIN_MOMENTUM_5S if attempts == 0 else REENTRY_MIN_MOMENTUM_5S
        return score5 >= threshold

    def _monitor_price_for_side(self, snap: MarketSnapshot, side: Side) -> float:
        # Execution-consistent monitoring proxy for open position management:
        # use best bid (sellable price); fallback to ref if bid unavailable.
        bid = snap.bids.get(side, 0.0)
        return bid if bid > 0 else snap.ref_prices[side]

    def _evaluate_side(self, side: Side, snap: MarketSnapshot) -> SideEvaluation:
        ws = self.window_state
        attempts = ws.attempt_count_per_side.get(side, 0) if ws else 0
        momentum_score = self._momentum_score(side, snap.timestamp, snap.prices[side], attempts=attempts)
        momentum_ok = self._momentum_ok_5s(side, snap.timestamp, snap.prices[side]) and self._momentum_passes_threshold(
            attempts, momentum_score
        )
        reason: Optional[str] = None
        if ws and ws.active:
            reason = "window already has active position"
        elif attempts >= MAX_ATTEMPTS_PER_SIDE_PER_WINDOW:
            reason = f"attempt limit reached ({attempts})"
        elif ws and ws.same_side_cooldown_until.get(side, 0.0) > snap.timestamp:
            reason = "same-side cooldown active"
        else:
            reason = self._entry_block_reason(snap=snap, side=side, momentum_ok=momentum_ok)
        return SideEvaluation(
            side=side,
            eligible=reason is None,
            reason=reason or "ok",
            momentum_ok=momentum_ok,
            momentum_score=momentum_score,
            buy_price=snap.buy_prices[side],
            ref_price=snap.ref_prices[side],
        )

    def _info_side_evals(self, snap: MarketSnapshot, up_eval: SideEvaluation, down_eval: SideEvaluation, chosen: Optional[Side]) -> None:
        now = time.time()
        if now - self.last_no_entry_info_at < NO_ENTRY_LOG_EVERY_SEC:
            return
        self.last_no_entry_info_at = now
        logger.info(
            "side-eval chosen=%s ttc=%.1fs up_reason=%s down_reason=%s up_buy=%.4f down_buy=%.4f "
            "up_ref=%.4f down_ref=%.4f up_momentum_score=%s down_momentum_score=%s "
            "up_momentum_threshold_passed=%s down_momentum_threshold_passed=%s",
            chosen or "none",
            snap.time_to_close_sec,
            up_eval.reason,
            down_eval.reason,
            up_eval.buy_price,
            down_eval.buy_price,
            up_eval.ref_price,
            down_eval.ref_price,
            f"{up_eval.momentum_score:+.6f}" if up_eval.momentum_score != float("-inf") else "None",
            f"{down_eval.momentum_score:+.6f}" if down_eval.momentum_score != float("-inf") else "None",
            up_eval.momentum_ok,
            down_eval.momentum_ok,
        )

    def _can_enter(self, snap: MarketSnapshot, side: Side) -> Tuple[bool, str]:
        assert self.window_state is not None
        ws = self.window_state

        if ws.active:
            return False, "window already has active position"

        attempts = ws.attempt_count_per_side.get(side, 0)
        if attempts >= MAX_ATTEMPTS_PER_SIDE_PER_WINDOW:
            return False, f"attempt limit reached ({attempts})"

        now = snap.timestamp
        if ws.same_side_cooldown_until.get(side, 0.0) > now:
            return False, "same-side cooldown active"

        attempts = ws.attempt_count_per_side.get(side, 0)
        momentum_score = self._momentum_score(side, now, snap.prices[side], attempts=attempts)
        momentum_ok = self._momentum_ok_5s(side, now, snap.prices[side]) and self._momentum_passes_threshold(
            attempts, momentum_score
        )
        block_reason = self._entry_block_reason(snap=snap, side=side, momentum_ok=momentum_ok)
        if block_reason:
            return False, block_reason

        return True, "ok"

    def _entry_block_reason(self, snap: MarketSnapshot, side: Side, momentum_ok: bool) -> Optional[str]:
        # Required priority:
        # 1) warmup_not_enough_history
        # 2) momentum_absent
        # 3) book_not_tradeable
        # 4) price_out_of_range
        # 5) near_expiry
        now = snap.timestamp
        if (now - self.market_changed_at) < MARKET_WARMUP_SEC:
            return f"warmup_not_enough_history ({now - self.market_changed_at:.1f}/{MARKET_WARMUP_SEC:.1f}s)"

        p5 = self._price_at_or_before(self.price_history[side], now - 5)
        if p5 is None:
            return "warmup_not_enough_history (need 5s history)"

        if not momentum_ok:
            return "momentum_absent"

        # Book/liquidity sanity before execution-price range checks.
        side_bid = snap.bids[side]
        side_ask = snap.asks[side]
        if side_ask <= 0 or side_bid <= 0:
            return "book_not_tradeable (missing_bid_or_ask)"
        if side_ask >= MAX_ENTRY_ASK_PRICE:
            return f"book_not_tradeable (ask_extreme={side_ask:.4f} >= {MAX_ENTRY_ASK_PRICE:.2f})"
        if side_bid <= MIN_ENTRY_BID_PRICE and side_ask >= (1.0 - MIN_ENTRY_BID_PRICE):
            return (
                "book_not_tradeable "
                f"(pathological bid={side_bid:.4f} ask={side_ask:.4f})"
            )
        if snap.top_ask_sizes[side] <= 0 or snap.top_bid_sizes[side] <= 0:
            return "book_not_tradeable (zero_top_size)"
        if (
            snap.bids["up"] <= MIN_ENTRY_BID_PRICE
            and snap.asks["up"] >= (1.0 - MIN_ENTRY_BID_PRICE)
            and snap.bids["down"] <= MIN_ENTRY_BID_PRICE
            and snap.asks["down"] >= (1.0 - MIN_ENTRY_BID_PRICE)
        ):
            return "book_not_tradeable (both_sides_pathological)"

        price = snap.buy_prices[side]
        if not (ENTRY_MIN_PRICE <= price <= ENTRY_MAX_PRICE):
            return f"price_out_of_range ({price:.4f} not in [{ENTRY_MIN_PRICE:.2f}, {ENTRY_MAX_PRICE:.2f}])"

        if snap.time_to_close_sec < NO_NEW_ENTRY_IF_TTC_LT_SEC:
            return f"near_expiry ({snap.time_to_close_sec:.1f}s < {NO_NEW_ENTRY_IF_TTC_LT_SEC}s)"

        return None

    def _signal_side(self, snap: MarketSnapshot) -> Optional[Side]:
        """
        Outcome-token momentum signal:
        - Up signal: up token is rising vs 5s ago.
        - Down signal: down token is rising vs 5s ago.
        If both qualify, pick stronger relative 5s momentum.
        """
        up_now, up_5, up_10 = self._momentum_debug_values("up", snap.timestamp)
        dn_now, dn_5, dn_10 = self._momentum_debug_values("down", snap.timestamp)
        up_ok = self._momentum_ok_5s("up", snap.timestamp, snap.prices["up"])
        down_ok = self._momentum_ok_5s("down", snap.timestamp, snap.prices["down"])

        logger.debug(
            "momentum debug: up[now=%.4f,5s=%s,10s=%s,ok5=%s] down[now=%.4f,5s=%s,10s=%s,ok5=%s]",
            up_now if up_now is not None else -1.0,
            f"{up_5:.4f}" if up_5 is not None else "None",
            f"{up_10:.4f}" if up_10 is not None else "None",
            up_ok,
            dn_now if dn_now is not None else -1.0,
            f"{dn_5:.4f}" if dn_5 is not None else "None",
            f"{dn_10:.4f}" if dn_10 is not None else "None",
            down_ok,
        )
        if up_ok and down_ok:
            up_delta = (snap.prices["up"] - (up_5 or snap.prices["up"])) / max((up_5 or 1e-6), 1e-6)
            dn_delta = (snap.prices["down"] - (dn_5 or snap.prices["down"])) / max((dn_5 or 1e-6), 1e-6)
            return "up" if up_delta >= dn_delta else "down"
        if up_ok:
            return "up"
        if down_ok:
            return "down"
        return None

    def _open_position(self, snap: MarketSnapshot, side: Side) -> None:
        assert self.window_state is not None
        entry_price = snap.buy_prices[side]
        open_order_id = self.exec.open_position(side=side, size=POSITION_SIZE, entry_price=entry_price)

        sl_price = entry_price * (1.0 - SL_PCT)
        sl_order_id = self.exec.place_protective_sl(side=side, size=POSITION_SIZE, sl_price=sl_price)
        # TODO(live-4): Persist live order IDs for open/sl orders in state.
        # TODO(live-6): Handle reject / partial fill / timeout / retry.
        # TODO(live-7): Block new entries until previous order status is confirmed.

        self.window_state.active = True
        self.window_state.current_position = PositionState(
            window_id=snap.window_id,
            side=side,
            entry_price=entry_price,
            entry_time=snap.timestamp,
            size=POSITION_SIZE,
        )
        self.trade_log.log(
            {
                "event": "entry",
                "window_id": snap.window_id,
                "side": side,
                "entry_price": entry_price,
                "entry_buy_price": snap.buy_prices[side],
                "entry_ref_price": snap.ref_prices[side],
                "current_monitor_price_for_sl": self._monitor_price_for_side(snap, side),
                "up_bid": snap.bids["up"],
                "up_ask": snap.asks["up"],
                "down_bid": snap.bids["down"],
                "down_ask": snap.asks["down"],
                "up_spread": snap.spreads["up"],
                "down_spread": snap.spreads["down"],
                "up_top_bid_size": snap.top_bid_sizes["up"],
                "up_top_ask_size": snap.top_ask_sizes["up"],
                "down_top_bid_size": snap.top_bid_sizes["down"],
                "down_top_ask_size": snap.top_ask_sizes["down"],
                "entry_time": snap.timestamp,
                "size": POSITION_SIZE,
                "attempt": self.window_state.attempt_count_per_side[side] + 1,
                "trailing_arm_at_return": TP_PCT,
                "trailing_drop_pct": TRAILING_DROP_PCT,
                "sl_pct": SL_PCT,
                "open_order_id": open_order_id,
                "sl_order_id": sl_order_id,
            }
        )
        self.session_stats.opened_total += 1
        self._save_state()

    def _close_position(self, snap: MarketSnapshot, reason: str, detail: Optional[Dict] = None) -> None:
        assert self.window_state is not None
        pos = self.window_state.current_position
        if not pos:
            return

        exit_price = self._monitor_price_for_side(snap, pos.side)
        pnl = (exit_price - pos.entry_price) / pos.entry_price
        if pos.side == "down":
            # Down token price still uses token mark-to-market; keep same formula.
            pnl = (exit_price - pos.entry_price) / pos.entry_price

        close_order_id = self.exec.close_position(side=pos.side, size=pos.size, reason=reason)

        self.window_state.attempt_count_per_side[pos.side] = (
            self.window_state.attempt_count_per_side.get(pos.side, 0) + 1
        )
        attempt_no = self.window_state.attempt_count_per_side[pos.side]
        self.window_state.last_exit_reason = reason
        self.window_state.last_exit_pnl = pnl
        holding_duration_sec = max(0.0, snap.timestamp - pos.entry_time)

        if reason == "trailing_profit_exit":
            self.window_state.same_side_cooldown_until[pos.side] = snap.timestamp + SAME_SIDE_COOLDOWN_SEC
            # TODO(live-5): after trailing-profit exit, cancel protective SL if still open.

        self.trade_log.log(
            {
                "event": "exit",
                "window_id": pos.window_id,
                "side": pos.side,
                "entry_price": pos.entry_price,
                "exit_price": exit_price,
                "exit_ref_price": snap.ref_prices[pos.side],
                "current_monitor_price_for_sl": exit_price,
                "up_bid": snap.bids["up"],
                "up_ask": snap.asks["up"],
                "down_bid": snap.bids["down"],
                "down_ask": snap.asks["down"],
                "up_spread": snap.spreads["up"],
                "down_spread": snap.spreads["down"],
                "up_top_bid_size": snap.top_bid_sizes["up"],
                "up_top_ask_size": snap.top_ask_sizes["up"],
                "down_top_bid_size": snap.top_bid_sizes["down"],
                "down_top_ask_size": snap.top_ask_sizes["down"],
                "entry_time": pos.entry_time,
                "exit_time": snap.timestamp,
                "holding_duration_sec": holding_duration_sec,
                "reason": reason,
                "reason_detail": detail or {},
                "computed_return_used_by_sl": pnl,
                "pnl": pnl,
                "attempt_no": attempt_no,
                "was_reentry": attempt_no > 1,
                "max_favorable_excursion": pos.max_favorable_return,
                "max_adverse_excursion": pos.max_adverse_return,
                "tp_armed": pos.trailing_armed,
                "close_order_id": close_order_id,
            }
        )
        self.session_trade_stats.append(
            {
                "side": pos.side,
                "pnl": pnl,
                "reason": reason,
                "attempt_no": attempt_no,
                "holding_duration_sec": holding_duration_sec,
            }
        )
        self.session_stats.closed_total += 1
        if reason == "sl":
            self.session_stats.closed_sl_total += 1
        if reason == "trailing_profit_exit":
            self.session_stats.closed_tp_total += 1

        self.window_state.current_position = None
        self.window_state.active = False
        # if position survived to another window, align state window after close
        self.window_state.window_id = snap.window_id
        self._save_state()

    def _manage_open_position(self, snap: MarketSnapshot) -> None:
        assert self.window_state and self.window_state.current_position
        pos = self.window_state.current_position
        current_price = self._monitor_price_for_side(snap, pos.side)
        ret = (current_price - pos.entry_price) / pos.entry_price
        pos.max_favorable_return = max(pos.max_favorable_return, ret)
        pos.max_adverse_return = min(pos.max_adverse_return, ret)

        if not pos.trailing_armed and ret >= TP_PCT:
            pos.trailing_armed = True
            pos.peak_price_since_trailing = current_price
            self.trade_log.log(
                {
                    "event": "trailing_armed",
                    "window_id": pos.window_id,
                    "side": pos.side,
                    "time": snap.timestamp,
                    "price": current_price,
                    "return": ret,
                }
            )
            self._save_state()

        # After trailing is armed, profit-protection has priority over ordinary SL logic.
        if pos.trailing_armed:
            pos.peak_price_since_trailing = max(pos.peak_price_since_trailing or current_price, current_price)
            trailing_threshold = pos.peak_price_since_trailing * (1.0 - TRAILING_DROP_PCT)
            if current_price <= trailing_threshold:
                self._close_position(
                    snap,
                    reason="trailing_profit_exit",
                    detail={
                        "trailing_armed": True,
                        "peak_price_since_trailing": pos.peak_price_since_trailing,
                        "trailing_threshold": trailing_threshold,
                        "current_monitor_price": current_price,
                        "chosen_exit_path": "trailing_profit_exit",
                        "monitor_price": current_price,
                        "entry_basis": "best_ask_buy_price",
                        "exit_basis": "best_bid_monitor_price",
                    },
                )
                return
            return

        effective_sl_trigger = max(0.0, SL_PCT - SL_EARLY_BUFFER_PCT)
        if (snap.timestamp - pos.entry_time) >= SL_GRACE_SEC and ret <= -effective_sl_trigger:
            self._close_position(
                snap,
                reason="sl",
                detail={
                    "sl_monitor_price": current_price,
                    "sl_return": ret,
                    "sl_trigger_pct": effective_sl_trigger,
                    "entry_basis": "best_ask_buy_price",
                    "exit_basis": "best_bid_monitor_price",
                    "sl_grace_sec": SL_GRACE_SEC,
                    "trailing_armed": False,
                    "chosen_exit_path": "sl",
                },
            )
            return

        # End of window: hold until resolution (as requested). No forced close here.

    def _maybe_log_strategy_summary(self) -> None:
        now = time.time()
        if now - self.last_summary_log_at < SUMMARY_LOG_EVERY_SEC:
            return
        self.last_summary_log_at = now
        trades = self.session_trade_stats
        if not trades:
            return
        total = len(trades)
        wins = sum(1 for t in trades if t["pnl"] > 0)
        avg_pnl = sum(t["pnl"] for t in trades) / total
        avg_by_side = {}
        avg_by_attempt = {}
        for key in ("up", "down"):
            pts = [t["pnl"] for t in trades if t["side"] == key]
            avg_by_side[key] = (sum(pts) / len(pts)) if pts else None
        for key in (1, 2, 3):
            pts = [t["pnl"] for t in trades if t["attempt_no"] == key]
            avg_by_attempt[key] = (sum(pts) / len(pts)) if pts else None
        sl_trades = [t["pnl"] for t in trades if t["reason"] == "sl"]
        avg_realized_sl = (sum(sl_trades) / len(sl_trades)) if sl_trades else None
        logger.info(
            "strategy-summary total=%d tp=%d sl=%d win_rate=%.2f avg_pnl=%.5f "
            "avg_up=%s avg_down=%s avg_attempt1=%s avg_attempt2=%s avg_attempt3=%s avg_realized_sl=%s",
            total,
            self.session_stats.closed_tp_total,
            self.session_stats.closed_sl_total,
            wins / total if total > 0 else 0.0,
            avg_pnl,
            f"{avg_by_side['up']:.5f}" if avg_by_side["up"] is not None else "None",
            f"{avg_by_side['down']:.5f}" if avg_by_side["down"] is not None else "None",
            f"{avg_by_attempt[1]:.5f}" if avg_by_attempt[1] is not None else "None",
            f"{avg_by_attempt[2]:.5f}" if avg_by_attempt[2] is not None else "None",
            f"{avg_by_attempt[3]:.5f}" if avg_by_attempt[3] is not None else "None",
            f"{avg_realized_sl:.5f}" if avg_realized_sl is not None else "None",
        )

    def run(self) -> None:
        while not self.stop_requested:
            try:
                if self.market is None:
                    now = time.time()
                    if now < self.next_discovery_at:
                        time.sleep(CHECK_INTERVAL_SEC)
                        continue
                    try:
                        self.market = self.data.discover_market(
                            BTC_5M_QUERY,
                            strict_slug=BTC_5M_SLUG,
                        )
                        self._on_market_changed(self.market)
                        self.discovery_fail_count = 0
                        self.next_discovery_at = 0.0
                        logger.info(
                            "Using market id=%s slug=%s question=%s",
                            self.market.get("id"),
                            self.market.get("slug"),
                            self.market.get("question"),
                        )
                    except Exception as exc:
                        self.discovery_fail_count += 1
                        logger.error("Market discovery failed, will retry: %s", exc)
                        self.next_discovery_at = time.time() + max(DISCOVERY_RETRY_SEC, CHECK_INTERVAL_SEC)
                        time.sleep(CHECK_INTERVAL_SEC)
                        continue

                snap = self.data.fetch_snapshot(self.market)
                self._ensure_window_state(snap.window_id)
                if self._market_should_rediscover(snap):
                    current_end_ts = self.data._extract_end_ts(self.market) if self.market else time.time()
                    logger.info(
                        "Invalidating near-expiry market id=%s slug=%s ttc=%.2fs",
                        self.market.get("id") if self.market else None,
                        self.market.get("slug") if self.market else None,
                        snap.time_to_close_sec,
                    )
                    if self.market:
                        self.data.mark_candidate_rejected(self.market, "near_expiry_current_window")
                    self.market = None
                    self.market_key = None
                    self.next_discovery_at = max(self.next_discovery_at, current_end_ts + 0.5)
                    continue

                for side in ("up", "down"):
                    self.price_history[side].append((snap.timestamp, snap.prices[side]))

                if self.window_state and self.window_state.active and self.window_state.current_position:
                    self._manage_open_position(snap)
                else:
                    up_eval = self._evaluate_side("up", snap)
                    down_eval = self._evaluate_side("down", snap)
                    eligible = [ev for ev in (up_eval, down_eval) if ev.eligible]
                    if not eligible:
                        self._info_side_evals(snap, up_eval, down_eval, chosen=None)
                    else:
                        chosen_eval = eligible[0]
                        if len(eligible) == 2:
                            chosen_eval = up_eval if up_eval.momentum_score >= down_eval.momentum_score else down_eval
                        self._info_side_evals(snap, up_eval, down_eval, chosen=chosen_eval.side)
                        self._open_position(snap, chosen_eval.side)
                logger.info(
                    "session_stats opened=%d closed=%d tp=%d sl=%d",
                    self.session_stats.opened_total,
                    self.session_stats.closed_total,
                    self.session_stats.closed_tp_total,
                    self.session_stats.closed_sl_total,
                )
                self._maybe_log_strategy_summary()

                if self.window_state and self.window_state.active and self.window_state.current_position:
                    time.sleep(max(0.05, OPEN_POSITION_CHECK_INTERVAL_SEC))
                else:
                    time.sleep(CHECK_INTERVAL_SEC)
            except InvalidMarketCandidate as exc:
                logger.error("Invalid market candidate: %s", exc)
                if self.market:
                    self.data.mark_candidate_rejected(self.market, str(exc))
                self.market = None
                self.market_key = None
                self.next_discovery_at = time.time() + max(DISCOVERY_RETRY_SEC, CHECK_INTERVAL_SEC)
                time.sleep(CHECK_INTERVAL_SEC)
            except OrderbookUnavailable as exc:
                logger.error("Orderbook unavailable, rejecting market: %s", exc)
                if self.market:
                    self.data.mark_candidate_rejected(self.market, str(exc))
                self.market = None
                self.market_key = None
                self.next_discovery_at = time.time() + max(DISCOVERY_RETRY_SEC, CHECK_INTERVAL_SEC)
                time.sleep(CHECK_INTERVAL_SEC)
            except DiscoveryError as exc:
                logger.error("Discovery error: %s", exc)
                self.market = None
                self.market_key = None
                self.next_discovery_at = time.time() + max(DISCOVERY_RETRY_SEC, CHECK_INTERVAL_SEC)
                time.sleep(CHECK_INTERVAL_SEC)
            except SnapshotError as exc:
                logger.error("Snapshot error: %s", exc)
                self.market = None
                self.market_key = None
                time.sleep(min(max(CHECK_INTERVAL_SEC, 1.0), 5.0))
            except Exception as exc:
                logger.exception("Main loop error: %s", exc)
                time.sleep(min(max(CHECK_INTERVAL_SEC, 1.0), 5.0))


def main() -> int:
    if not DRY_RUN and not PRIVATE_KEY:
        raise RuntimeError("POLYMARKET_PRIVATE_KEY is required when POLYMARKET_DRY_RUN=0")
    bot = BeethovenV1Bot()

    def _stop(_signum, _frame):
        bot.stop_requested = True
        logger.info("Stop requested")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    logger.info(
        "Starting Beethoven v1 bot: dry_run=%s market_slug=%s scan_interval=%ss",
        DRY_RUN,
        BTC_5M_SLUG,
        CHECK_INTERVAL_SEC,
    )
    bot.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())