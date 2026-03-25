# XManAI

## Logging for `weather_combined_bot.py`

`weather_combined_bot.py` writes logs to file by default:

- default path: `./logs/combined_YYYY-MM-DD.log` (UTC date).
- `POLYMARKET_LOG_FILE` — custom path to append runtime logs.
- `POLYMARKET_DISABLE_FILE_LOG=1` — disable file logging completely.
- default buy window: today + tomorrow (`LOOKAHEAD_DAYS=2`).
- default market-volume filter: `>= 1000 USD`.
- date matching tolerates UTC offset (`endDate` and `endDate - N days`) and also checks date phrase in market title/question (e.g. `on March 24`).
  Tune via `POLYMARKET_DATE_SHIFT_TOLERANCE_DAYS` (default `2`).

When file logging is enabled, the bot writes logs to both console and file.
After each cycle, bot prints `SCAN COMPLETE ... | sleep=Ns`, then waits `CHECK_INTERVAL` seconds before next scan (default `5s`).

### PowerShell example

```powershell
$env:POLYMARKET_LOG_FILE = "C:\\XManAI\\ACrypto\\polymarket-bot\\logs\\combined_$(Get-Date -Format yyyy-MM-dd).log"
python .\weather_combined_bot.py
```

### Bash example

```bash
export POLYMARKET_LOG_FILE="./logs/combined_$(date +%F).log"
python weather_combined_bot.py
```

## Run faster (debug mode)

For faster visible cycles, reduce poll interval, request timeout and number of markets processed per scan.

### PowerShell fast run example

```powershell
$env:POLYMARKET_CHECK_INTERVAL_SEC = "5"
$env:POLYMARKET_REQUEST_TIMEOUT_SEC = "8"
$env:POLYMARKET_ORDERBOOK_TIMEOUT_SEC = "4"
$env:POLYMARKET_MAX_MARKETS_PER_SCAN = "25"
$env:POLYMARKET_ENABLE_BUY_SCAN = "0"
python .\weather_combined_bot.py
```

`POLYMARKET_ENABLE_BUY_SCAN=0` is useful if bot appears stuck after SELL checks: it skips new-entry scan and keeps only sell/TP/SL monitoring.
`POLYMARKET_ORDERBOOK_TIMEOUT_SEC` limits a single `get_order_book` call to avoid hard stalls on one token.
`POLYMARKET_BUY_SCAN_MAX_DURATION_SEC` limits total buy-market discovery time per cycle.
`POLYMARKET_SIMPLE_ENTRY_MODE=1` (default) keeps only base entry logic: 2-day window, entry price range, and 1h-up trend; spread/volume/pullback filters are disabled.
`POLYMARKET_MARKET_SEARCH` (default `highest temperature`) narrows Gamma query to highest-temperature markets (not generic temperature-increase markets).
After market discovery, bot prints `BUY SCAN FILTERS | ...` with counts of skipped markets by reason (including `skip_no_yes_outcome`).
If strict weather matching fails completely, fallback scan uses a weaker `highest temperature` text filter (not an unrestricted all-markets scan).
If search returns no weather rows, bot retries `query_mode=events_by_tag` (Gamma tags/events), then `query_mode=event_slug`, then `query_mode=slug`.
If those fail, bot tries `query_mode=site_events` by scraping market slugs from `https://polymarket.com/weather/temperature`.
You can force site-events discovery from specific pages via `POLYMARKET_EVENT_URLS` (comma-separated event URLs). If unset, bot falls back to a built-in starter list (Seoul/Tokyo/Shanghai/Wellington/London for March 25, 2026).
If Gamma tags API is unavailable or returns no weather tags, bot auto-skips to `query_mode=event_slug`.

## Daily summary helper

Use `scripts/daily_summary.ps1` to parse a daily log and produce a compact trading summary.

### Usage

```powershell
.\scripts\daily_summary.ps1 -LogPath .\logs\combined_2026-03-24.log
```

The script prints summary counters and appends them to `./logs/daily_summary.csv`.
