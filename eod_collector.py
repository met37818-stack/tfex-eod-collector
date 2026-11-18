#!/usr/bin/env python3
"""
Stateless EOD Data Collector for TFEX & BOT THOR Rates.
Collects End-of-Day market data from 5 sources:
1. Investor Types (eod_investor_types)
2. Market Statistics (eod_market_stats)
3. Active Futures Series (eod_futures_series)
4. Active Options Series (eod_options_series)
5. BOT THOR Interest Rates (eod_thor_rates)

Persists records idempotently via Turso Cloud SQLite HTTP Pipeline API (/v2/pipeline)
or local SQLite database fallback.
"""

import os
import sys
import re
import json
import time
import calendar
import argparse
import datetime
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3

import requests
from bs4 import BeautifulSoup
import pytz

# ==============================================================================
# CONFIGURATION & CONSTANTS
# ==============================================================================

BKK_TZ = pytz.timezone("Asia/Bangkok")

# Official SET Holidays (2026 - 2027)
SET_HOLIDAYS = {
    # 2026
    "2026-01-01",  # New Year's Day
    "2026-01-02",  # New Year Special Holiday
    "2026-03-03",  # Makha Bucha Day
    "2026-04-06",  # Chakri Memorial Day
    "2026-04-13",  # Songkran Festival
    "2026-04-14",  # Songkran Festival
    "2026-04-15",  # Songkran Festival
    "2026-05-01",  # National Labour Day
    "2026-05-04",  # Coronation Day
    "2026-06-01",  # Visakha Bucha Day
    "2026-06-03",  # H.M. Queen Suthida's Birthday
    "2026-07-28",  # H.M. King Maha Vajiralongkorn's Birthday
    "2026-07-29",  # Asahna Bucha Day
    "2026-08-12",  # H.M. Queen Sirikit The Queen Mother's Birthday
    "2026-10-13",  # King Bhumibol Adulyadej Memorial Day
    "2026-10-23",  # Chulalongkorn Day
    "2026-12-07",  # Substitution for King Bhumibol's Birthday
    "2026-12-10",  # Constitution Day
    "2026-12-31",  # New Year's Eve
    # 2027
    "2027-01-01",  # New Year's Day
    "2027-02-22",  # Makha Bucha Day
    "2027-04-06",  # Chakri Memorial Day
    "2027-04-13",  # Songkran Festival
    "2027-04-14",  # Songkran Festival
    "2027-04-15",  # Songkran Festival
    "2027-05-03",  # Substitution for National Labour Day
    "2027-05-04",  # Coronation Day
    "2027-05-20",  # Visakha Bucha Day
    "2027-06-03",  # H.M. Queen Suthida's Birthday
    "2027-07-19",  # Asahna Bucha Day
    "2027-07-28",  # H.M. King Maha Vajiralongkorn's Birthday
    "2027-08-12",  # H.M. Queen Sirikit The Queen Mother's Birthday
    "2027-10-13",  # King Bhumibol Adulyadej Memorial Day
    "2027-10-25",  # Substitution for Chulalongkorn Day
    "2027-12-06",  # Substitution for King Bhumibol's Birthday
    "2027-12-10",  # Constitution Day
    "2027-12-31",  # New Year's Eve
}

BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,th;q=0.8",
    "Referer": "https://www.tfex.co.th/en/market-data/historical-data/trading-statistics-by-series",
}

# ==============================================================================
# ENVIRONMENT & HELPER FUNCTIONS
# ==============================================================================

def load_dotenv(path: str = ".env"):
    """Lightweight .env loader without external dependencies."""
    search_paths = [path, os.path.join(os.path.dirname(__file__), path)]
    for p in search_paths:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if "=" in line:
                            k, v = line.split("=", 1)
                            k = k.strip()
                            v = v.strip().strip("'\"")
                            if k not in os.environ:
                                os.environ[k] = v
                break
            except Exception as e:
                print(f"[WARN] Could not parse .env at {p}: {e}")

def get_last_thursday(year: int, month: int) -> int:
    """Find the day of the last Thursday of a given month."""
    _, last_day = calendar.monthrange(year, month)
    for day in range(last_day, 0, -1):
        if calendar.weekday(year, month, day) == calendar.THURSDAY:
            return day
    raise ValueError(f"Could not determine last Thursday for {year}-{month:02d}")

def get_active_quarterly_series(ref_time: datetime.datetime = None) -> list[str]:
    """
    Calculate the 2 active quarterly SET50 series (H: Mar, M: Jun, U: Sep, Z: Dec).
    Expiry: Last Thursday of the quarter month at 16:30 ICT.
    """
    if ref_time is None:
        ref_time = datetime.datetime.now(BKK_TZ)
    elif ref_time.tzinfo is None:
        ref_time = BKK_TZ.localize(ref_time)
    else:
        ref_time = ref_time.astimezone(BKK_TZ)

    quarter_months = [3, 6, 9, 12]
    month_codes = {3: "H", 6: "M", 9: "U", 12: "Z"}
    active_series = []

    for yr in range(ref_time.year, ref_time.year + 3):
        for m in quarter_months:
            last_thu = get_last_thursday(yr, m)
            expiry_dt = BKK_TZ.localize(datetime.datetime(yr, m, last_thu, 16, 30, 0))
            if ref_time <= expiry_dt:
                series_code = f"S50{month_codes[m]}{str(yr)[-2:]}"
                active_series.append(series_code)
                if len(active_series) == 2:
                    return active_series
    return active_series

def is_holiday_or_weekend(ref_time: datetime.datetime = None) -> tuple[bool, str]:
    """
    Determine if target trade session is a weekend or SET holiday.
    If run before 09:00 ICT, target session is T-1.
    If run at or after 09:00 ICT, target session is T.
    """
    if ref_time is None:
        ref_time = datetime.datetime.now(BKK_TZ)
    elif ref_time.tzinfo is None:
        ref_time = BKK_TZ.localize(ref_time)
    else:
        ref_time = ref_time.astimezone(BKK_TZ)

    if ref_time.hour < 9:
        # On Monday morning before 09:00 ICT, the relevant trade session is Friday (T-3)
        if ref_time.weekday() == 0:
            target_date = (ref_time - datetime.timedelta(days=3)).date()
        else:
            target_date = (ref_time - datetime.timedelta(days=1)).date()
    else:
        target_date = ref_time.date()

    target_str = target_date.strftime("%Y-%m-%d")
    weekday_name = target_date.strftime("%A")

    if target_date.weekday() >= 5:
        return True, f"Target trade date {target_str} is a weekend ({weekday_name})"
    if target_str in SET_HOLIDAYS:
        return True, f"Target trade date {target_str} is an official SET holiday"

    return False, f"Target trade date {target_str} is a valid trading session"

def clean_int(val) -> int:
    """Safe integer parser handling commas, signs, and blanks."""
    if val is None:
        return 0
    s = str(val).replace(",", "").replace("+", "").strip()
    if s in ("", "-", "None", "null"):
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0

def clean_float(val) -> float | None:
    """Safe float parser handling commas and blanks."""
    if val is None:
        return None
    s = str(val).replace(",", "").strip()
    if s in ("", "-", "None", "null"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

# ==============================================================================
# DATA COLLECTORS (SECTIONS 1 - 5)
# ==============================================================================

def init_tfex_session() -> requests.Session:
    """Initialize a requests session and visit TFEX page to acquire cookies."""
    session = requests.Session()
    session.headers.update(BASE_HEADERS)
    init_url = "https://www.tfex.co.th/en/market-data/historical-data/trading-statistics-by-series"
    try:
        resp = session.get(init_url, timeout=15)
        if resp.status_code == 200:
            print("[TFEX Session] Initialized successfully. Cookies established.")
        else:
            print(f"[TFEX Session] Warning: Initial page status {resp.status_code}")
    except Exception as e:
        print(f"[TFEX Session] Warning: Failed to pre-fetch cookies: {e}")
    return session

def fetch_investor_types(session: requests.Session) -> list[dict]:
    """
    Section 1: Investor Types (eod_investor_types).
    Scrapes HTML from https://www.tfex.co.th/en/market-data/historical-data/trading-by-investor-types
    """
    url = "https://www.tfex.co.th/en/market-data/historical-data/trading-by-investor-types"
    print(f"[Section 1] Fetching Investor Types from {url}...")
    resp = session.get(url, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    trade_date = None

    # Parse As of date
    for text in soup.stripped_strings:
        m = re.search(r"As\s+of\s+(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})", text)
        if m:
            trade_date = datetime.datetime.strptime(m.group(1), "%d %b %Y").strftime("%Y-%m-%d")
            break

    if not trade_date:
        raise ValueError("Could not parse 'As of' date from trading-by-investor-types page")

    target_categories = {
        "Equity Index Futures",
        "Equity Index Call Options",
        "Equity Index Put Options",
    }

    results = []
    table = soup.find("table")
    if not table:
        raise ValueError("No table found on trading-by-investor-types page")

    for tr in table.find_all("tr"):
        cols = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
        if not cols:
            continue
        cat_name = cols[0]
        if cat_name in target_categories:
            results.append({
                "trade_date": trade_date,
                "category": cat_name,
                "inst_long": clean_int(cols[1]),
                "inst_short": clean_int(cols[2]),
                "inst_net": clean_int(cols[3]),
                "foreign_long": clean_int(cols[4]),
                "foreign_short": clean_int(cols[5]),
                "foreign_net": clean_int(cols[6]),
                "local_long": clean_int(cols[7]),
                "local_short": clean_int(cols[8]),
                "local_net": clean_int(cols[9]),
                "total_volume": clean_int(cols[10]) if len(cols) > 10 else 0,
            })

    print(f"[Section 1] Extracted {len(results)} investor type records for {trade_date}.")
    return results

def fetch_market_stats(session: requests.Session) -> list[dict]:
    """
    Section 2: Market Statistics (eod_market_stats).
    Fetches JSON API for TXI_F/SET50_FC, TXI_C/SET50_OEC, TXI_P/SET50_OEP (last 7 days).
    Note: API requires dates in DD/MM/YYYY format.
    """
    now = datetime.datetime.now(BKK_TZ)
    from_date_str = (now - datetime.timedelta(days=7)).strftime("%d/%m/%Y")
    to_date_str = now.strftime("%d/%m/%Y")

    categories = [
        ("TXI_F", "SET50_FC"),
        ("TXI_C", "SET50_OEC"),
        ("TXI_P", "SET50_OEP"),
    ]

    results = []
    headers = {
        "User-Agent": BASE_HEADERS["User-Agent"],
        "Referer": "https://www.tfex.co.th/en/market-data/historical-data/trading-statistics-by-series",
        "Accept": "application/json, text/plain, */*",
    }

    for market_id, inst_id in categories:
        api_url = (
            f"https://www.tfex.co.th/api/set/tfex/historical-statistics/daily"
            f"?website=tfex&fromDate={from_date_str}&toDate={to_date_str}"
            f"&marketListId={market_id}&instrumentId={inst_id}&trade=all"
        )
        print(f"[Section 2] Fetching Market Stats for {market_id}/{inst_id}...")
        resp = session.get(api_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"[Section 2] Warning: Failed to fetch {market_id}/{inst_id} ({resp.status_code})")
            continue

        data = resp.json().get("data", [])
        for item in data:
            raw_date = item.get("date")
            if not raw_date:
                continue
            trade_date = raw_date[:10]
            results.append({
                "trade_date": trade_date,
                "market_id": market_id,
                "instrument_id": inst_id,
                "open": clean_float(item.get("open")),
                "high": clean_float(item.get("high")),
                "low": clean_float(item.get("low")),
                "settlement_price": clean_float(item.get("settlementPrice")),
                "chg": clean_float(item.get("change")),
                "pct_chg": clean_float(item.get("percentChange")),
                "volume": clean_int(item.get("volume")),
                "oi": clean_int(item.get("oi")),
            })

    print(f"[Section 2] Extracted {len(results)} market stats records across 3 categories.")
    return results

def fetch_futures_series(session: requests.Session, active_series: list[str]) -> list[dict]:
    """
    Section 3: Active Futures Series (eod_futures_series).
    Fetches JSON API for 2 active series (last 5 trading days).
    """
    results = []
    headers = {
        "User-Agent": BASE_HEADERS["User-Agent"],
        "Referer": "https://www.tfex.co.th/en/market-data/historical-data/trading-statistics-by-series",
        "Accept": "application/json, text/plain, */*",
    }

    for series in active_series:
        api_url = f"https://www.tfex.co.th/api/set/tfex/series/{series}/historical-trading?website=tfex&lang=en"
        print(f"[Section 3] Fetching Futures Series {series}...")
        resp = session.get(api_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"[Section 3] Warning: Failed to fetch series {series} ({resp.status_code})")
            continue

        data = resp.json().get("data", [])
        # Take the most recent 5 records
        for item in data[:5]:
            raw_date = item.get("date")
            if not raw_date:
                continue
            trade_date = raw_date[:10]
            results.append({
                "trade_date": trade_date,
                "series": series,
                "open": clean_float(item.get("open")),
                "high": clean_float(item.get("high")),
                "low": clean_float(item.get("low")),
                "close": clean_float(item.get("close")),
                "settlement_price": clean_float(item.get("settlementPrice")),
                "chg": clean_float(item.get("change")),
                "pct_chg": clean_float(item.get("percentChange")),
                "volume": clean_int(item.get("totalVolume")),
                "oi": clean_int(item.get("oi")),
            })

    print(f"[Section 3] Extracted {len(results)} futures series records for {active_series}.")
    return results

def fetch_options_series(session: requests.Session, active_series: list[str]) -> list[dict]:
    """
    Section 4: Active Options Series (eod_options_series).
    Scrapes contract symbols for active series, then concurrently fetches 5 days history
    with ThreadPoolExecutor(8 workers).
    """
    market_page_url = "https://www.tfex.co.th/en/products/equity/set50-index-options/market-data"
    print(f"[Section 4] Discovering option symbols from {market_page_url}...")
    resp = session.get(market_page_url, timeout=15)
    resp.raise_for_status()

    # Regex pattern matching active series options, e.g. (?:S50Z26|S50H27)[CP]\d+
    series_regex_pattern = "|".join(re.escape(s) for s in active_series)
    contract_pattern = re.compile(rf"(?:{series_regex_pattern})[CP]\d+")
    symbols = sorted(list(set(contract_pattern.findall(resp.text))))
    print(f"[Section 4] Discovered {len(symbols)} option contracts for {active_series}.")

    headers = {
        "User-Agent": BASE_HEADERS["User-Agent"],
        "Referer": "https://www.tfex.co.th/en/market-data/historical-data/trading-statistics-by-series",
        "Accept": "application/json, text/plain, */*",
    }

    parse_regex = re.compile(r"^(S50[HMUZ]\d{2})([CP])(\d+)$")
    results = []

    def fetch_single_option(sym: str) -> list[dict]:
        match = parse_regex.match(sym)
        if not match:
            return []
        ser, opt_type, strike_str = match.groups()
        strike_val = float(strike_str)

        opt_url = f"https://www.tfex.co.th/api/set/tfex/series/{sym}/historical-trading?website=tfex&lang=en"
        try:
            r = session.get(opt_url, headers=headers, timeout=12)
            if r.status_code != 200:
                return []
            data = r.json().get("data", [])
            opt_records = []
            for item in data[:5]:
                raw_date = item.get("date")
                if not raw_date:
                    continue
                opt_records.append({
                    "trade_date": raw_date[:10],
                    "symbol": sym,
                    "series": ser,
                    "option_type": opt_type,
                    "strike": strike_val,
                    "open": clean_float(item.get("open")),
                    "high": clean_float(item.get("high")),
                    "low": clean_float(item.get("low")),
                    "close": clean_float(item.get("close")),
                    "settlement_price": clean_float(item.get("settlementPrice")),
                    "chg": clean_float(item.get("change")),
                    "pct_chg": clean_float(item.get("percentChange")),
                    "volume": clean_int(item.get("totalVolume")),
                    "oi": clean_int(item.get("oi")),
                })
            return opt_records
        except Exception as e:
            return []

    print(f"[Section 4] Fetching historical trading for {len(symbols)} options with 8 workers...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_sym = {executor.submit(fetch_single_option, sym): sym for sym in symbols}
        for future in as_completed(future_to_sym):
            recs = future.result()
            if recs:
                results.extend(recs)

    print(f"[Section 4] Extracted {len(results)} option series records across {len(symbols)} contracts.")
    return results

def fetch_thor_rates() -> list[dict]:
    """
    Section 5: Bank of Thailand THOR Interest Rates (eod_thor_rates).
    Fetches HTML from https://app.bot.or.th/THOR/en/Home/GetTHORHistoricalLink
    Extracts ASP.NET viewstate and requests 30-45 days window.
    """
    url = "https://app.bot.or.th/THOR/en/Home/GetTHORHistoricalLink"
    print(f"[Section 5] Fetching THOR rates from {url}...")
    headers = {"User-Agent": BASE_HEADERS["User-Agent"]}
    session = requests.Session()
    r = session.get(url, headers=headers, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")

    post_soup = soup
    if form and form.get("action"):
        post_url = urllib.parse.urljoin(r.url, form.get("action"))
        now = datetime.datetime.now()
        prev = now - datetime.timedelta(days=45)

        vs = soup.find("input", {"id": "__VIEWSTATE"})
        vsg = soup.find("input", {"id": "__VIEWSTATEGENERATOR"})
        ev = soup.find("input", {"id": "__EVENTVALIDATION"})

        if vs and vsg and ev:
            payload = {
                "__VIEWSTATE": vs["value"],
                "__VIEWSTATEGENERATOR": vsg["value"],
                "__EVENTVALIDATION": ev["value"],
                "drpPeriod": "DAY",
                "drpFromMonth": f"xxxx{prev.month:02d}xx",
                "drpFromYear": f"{prev.year}xxxx",
                "drpToMonth": f"xxxx{now.month:02d}xx",
                "drpToYear": f"{now.year}xxxx",
                "btnSubmit": "Submit",
            }
            try:
                r2 = session.post(post_url, data=payload, headers=headers, timeout=15)
                if r2.status_code == 200:
                    post_soup = BeautifulSoup(r2.text, "html.parser")
            except Exception as e:
                print(f"[Section 5] Warning: Form POST failed, falling back to GET table: {e}")

    table = post_soup.find("table")
    if not table:
        raise ValueError("Could not find data table on BOT THOR page")

    rows = table.find_all("tr")
    if len(rows) < 6:
        raise ValueError("BOT THOR table does not have sufficient rows")

    header_cells = [c.get_text(" ", strip=True) for c in rows[0].find_all(["th", "td"])]
    r1 = [c.get_text(" ", strip=True) for c in rows[1].find_all(["th", "td"])]
    r3 = [c.get_text(" ", strip=True) for c in rows[3].find_all(["th", "td"])]
    r4 = [c.get_text(" ", strip=True) for c in rows[4].find_all(["th", "td"])]
    r5 = [c.get_text(" ", strip=True) for c in rows[5].find_all(["th", "td"])]

    thor_records = []
    # Dates start from column index 2
    for idx in range(2, len(header_cells)):
        date_str = header_cells[idx]
        try:
            dt = datetime.datetime.strptime(date_str, "%d %b %Y")
            trade_date = dt.strftime("%Y-%m-%d")
        except Exception:
            continue

        thor_records.append({
            "trade_date": trade_date,
            "thor_rate": clean_float(r1[idx]) if idx < len(r1) else None,
            "thor_1m_avg": clean_float(r3[idx]) if idx < len(r3) else None,
            "thor_3m_avg": clean_float(r4[idx]) if idx < len(r4) else None,
            "thor_6m_avg": clean_float(r5[idx]) if idx < len(r5) else None,
        })

    # Sort descending by trade_date and limit to last 30 business days
    thor_records.sort(key=lambda x: x["trade_date"], reverse=True)
    thor_records = thor_records[:30]

    print(f"[Section 5] Extracted {len(thor_records)} THOR interest rate records.")
    return thor_records

# ==============================================================================
# DATABASE MANAGEMENT & TURSO HTTP PIPELINE / SQLITE
# ==============================================================================

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS eod_investor_types (
        trade_date TEXT NOT NULL,
        category TEXT NOT NULL,
        inst_long INTEGER,
        inst_short INTEGER,
        inst_net INTEGER,
        foreign_long INTEGER,
        foreign_short INTEGER,
        foreign_net INTEGER,
        local_long INTEGER,
        local_short INTEGER,
        local_net INTEGER,
        total_volume INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, category)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS eod_market_stats (
        trade_date TEXT NOT NULL,
        market_id TEXT NOT NULL,
        instrument_id TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        settlement_price REAL,
        chg REAL,
        pct_chg REAL,
        volume INTEGER,
        oi INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, market_id, instrument_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS eod_futures_series (
        trade_date TEXT NOT NULL,
        series TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        settlement_price REAL,
        chg REAL,
        pct_chg REAL,
        volume INTEGER,
        oi INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, series)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS eod_options_series (
        trade_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        series TEXT NOT NULL,
        option_type TEXT NOT NULL,
        strike REAL NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        settlement_price REAL,
        chg REAL,
        pct_chg REAL,
        volume INTEGER,
        oi INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date, symbol)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS eod_thor_rates (
        trade_date TEXT NOT NULL,
        thor_rate REAL,
        thor_1m_avg REAL,
        thor_3m_avg REAL,
        thor_6m_avg REAL,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (trade_date)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS eod_telegram_log (
        trade_date TEXT NOT NULL PRIMARY KEY,
        notified_at_bkk TEXT NOT NULL,
        message_snippet TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS eod_options_greeks (
        trade_date TEXT NOT NULL,
        symbol TEXT NOT NULL,
        series TEXT NOT NULL,
        option_type TEXT NOT NULL,
        strike REAL NOT NULL,
        underlying_futures TEXT NOT NULL,
        dte INTEGER NOT NULL,
        thor_rate REAL,
        iv REAL,
        delta REAL,
        gamma REAL,
        theta REAL,
        vega REAL,
        vanna REAL,
        charm REAL,
        open_interest INTEGER,
        updated_at_bkk TEXT,
        PRIMARY KEY (trade_date, symbol)
    );
    """
]

def to_turso_arg(val) -> dict:
    """Format Python values into libSQL / Turso typed arguments."""
    if val is None:
        return {"type": "null"}
    elif isinstance(val, bool):
        return {"type": "integer", "value": "1" if val else "0"}
    elif isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    elif isinstance(val, float):
        return {"type": "float", "value": val}
    else:
        return {"type": "text", "value": str(val)}

def normalize_turso_url(raw_url: str) -> str:
    """Normalize Turso URL to HTTP Pipeline endpoint (/v2/pipeline)."""
    url = raw_url.strip()
    if url.startswith("libsql://"):
        url = "https://" + url[len("libsql://"):]
    elif not url.startswith("http://") and not url.startswith("https://"):
        url = "https://" + url
    url = url.rstrip("/")
    if not url.endswith("/v2/pipeline"):
        url = url + "/v2/pipeline"
    return url

def execute_turso_pipeline(pipeline_url: str, auth_token: str, statements: list[dict], batch_size: int = 100):
    """
    Execute batch statements against Turso Cloud SQLite via HTTP Pipeline API.
    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    total = len(statements)
    print(f"[Turso Pipeline] Sending {total} statements to {pipeline_url} in batches of {batch_size}...")

    for i in range(0, total, batch_size):
        chunk = statements[i:i + batch_size]
        requests_payload = []
        for stmt in chunk:
            sql = stmt["sql"].strip()
            args = stmt.get("args", [])
            turso_args = [to_turso_arg(a) for a in args]
            requests_payload.append({
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": turso_args
                }
            })
        requests_payload.append({"type": "close"})

        resp = requests.post(
            pipeline_url,
            headers=headers,
            json={"requests": requests_payload},
            timeout=30
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Turso Pipeline failed ({resp.status_code}): {resp.text}")

        res_json = resp.json()
        for idx, result in enumerate(res_json.get("results", [])):
            if result.get("type") == "error":
                raise RuntimeError(f"Turso statement execution error at index {idx}: {result.get('error')}")

    print(f"[Turso Pipeline] Successfully executed all {total} statements.")

def execute_sqlite_local(db_path: str, statements: list[dict]):
    """Execute statements against local SQLite database (for testing and fallback)."""
    print(f"[Local SQLite] Executing {len(statements)} statements on {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    for stmt in statements:
        sql = stmt["sql"].strip()
        args = stmt.get("args", [])
        cursor.execute(sql, args)
    conn.commit()
    conn.close()
    print(f"[Local SQLite] Successfully committed {len(statements)} statements to {db_path}.")

def build_upsert_statements(
    inv_types: list[dict],
    mkt_stats: list[dict],
    fut_series: list[dict],
    opt_series: list[dict],
    thor_rates: list[dict]
) -> list[dict]:
    """Generate idempotent INSERT OR REPLACE statements for all collected records."""
    statements = []

    # 1. DDL Statements
    for ddl in DDL_STATEMENTS:
        statements.append({"sql": ddl, "args": []})

    # 2. Section 1: Investor Types
    sql_inv = """
    INSERT OR REPLACE INTO eod_investor_types (
        trade_date, category, inst_long, inst_short, inst_net,
        foreign_long, foreign_short, foreign_net,
        local_long, local_short, local_net, total_volume
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for r in inv_types:
        statements.append({
            "sql": sql_inv,
            "args": [
                r["trade_date"], r["category"],
                r["inst_long"], r["inst_short"], r["inst_net"],
                r["foreign_long"], r["foreign_short"], r["foreign_net"],
                r["local_long"], r["local_short"], r["local_net"],
                r["total_volume"]
            ]
        })

    # 3. Section 2: Market Stats
    sql_mkt = """
    INSERT OR REPLACE INTO eod_market_stats (
        trade_date, market_id, instrument_id, open, high, low,
        settlement_price, chg, pct_chg, volume, oi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for r in mkt_stats:
        statements.append({
            "sql": sql_mkt,
            "args": [
                r["trade_date"], r["market_id"], r["instrument_id"],
                r["open"], r["high"], r["low"],
                r["settlement_price"], r["chg"], r["pct_chg"],
                r["volume"], r["oi"]
            ]
        })

    # 4. Section 3: Futures Series
    sql_fut = """
    INSERT OR REPLACE INTO eod_futures_series (
        trade_date, series, open, high, low, close,
        settlement_price, chg, pct_chg, volume, oi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for r in fut_series:
        statements.append({
            "sql": sql_fut,
            "args": [
                r["trade_date"], r["series"],
                r["open"], r["high"], r["low"], r["close"],
                r["settlement_price"], r["chg"], r["pct_chg"],
                r["volume"], r["oi"]
            ]
        })

    # 5. Section 4: Options Series
    sql_opt = """
    INSERT OR REPLACE INTO eod_options_series (
        trade_date, symbol, series, option_type, strike, open, high, low, close,
        settlement_price, chg, pct_chg, volume, oi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for r in opt_series:
        statements.append({
            "sql": sql_opt,
            "args": [
                r["trade_date"], r["symbol"], r["series"], r["option_type"], r["strike"],
                r["open"], r["high"], r["low"], r["close"],
                r["settlement_price"], r["chg"], r["pct_chg"],
                r["volume"], r["oi"]
            ]
        })

    # 6. Section 5: THOR Rates
    sql_thor = """
    INSERT OR REPLACE INTO eod_thor_rates (
        trade_date, thor_rate, thor_1m_avg, thor_3m_avg, thor_6m_avg
    ) VALUES (?, ?, ?, ?, ?);
    """
    for r in thor_rates:
        statements.append({
            "sql": sql_thor,
            "args": [
                r["trade_date"], r["thor_rate"],
                r["thor_1m_avg"], r["thor_3m_avg"], r["thor_6m_avg"]
            ]
        })

    return statements

# ==============================================================================
# FIFO INTRADAY RETENTION & STORAGE GUARD
# ==============================================================================

MAX_FREE_QUOTA_MB = 5120.0       # 5.0 GB free quota
TRIGGER_THRESHOLD_MB = 4096.0    # 4.0 GB (80% trigger threshold)
MIN_RETENTION_DAYS = 365         # Strict 1-year safety lock
PRUNE_CHUNK_DAYS = 7             # Purge 1 week per cycle
INTRADAY_TABLES = [
    "spot_snapshots",
    "futures_snapshots",
    "options_snapshots"
]

def run_fifo_retention_guard(pipeline_url: str = None, auth_token: str = None, db_file: str = None):
    """
    Automated FIFO Intraday Retention Guard.
    1. Inspects current DB size via PRAGMA page_count and page_size.
    2. Logs storage health relative to 5.0 GB (5,120 MB) quota.
    3. If size exceeds 4.0 GB (4,096 MB / 80%):
       - Searches oldest date across spot_snapshots, futures_snapshots, options_snapshots.
       - Enforces Safety Lock: Must be at least 365 days old (never delete recent data).
       - Deletes oldest data in 7-day chunks from intraday tables.
       - Reclaims space and logs updated size.
    * Note: eod_* tables are strictly untouched and preserved indefinitely.
    """
    now_bkk = datetime.datetime.now(BKK_TZ)
    current_date = now_bkk.date()

    page_count = 0
    page_size = 4096
    is_turso = bool(pipeline_url and auth_token)

    if is_turso:
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        reqs = [
            {"type": "execute", "stmt": {"sql": "PRAGMA page_count;"}},
            {"type": "execute", "stmt": {"sql": "PRAGMA page_size;"}},
            {"type": "close"}
        ]
        try:
            r = requests.post(pipeline_url, headers=headers, json={"requests": reqs}, timeout=20)
            if r.status_code == 200:
                res = r.json()
                page_count = int(res["results"][0]["response"]["result"]["rows"][0][0]["value"])
                page_size = int(res["results"][1]["response"]["result"]["rows"][0][0]["value"])
        except Exception as e:
            print(f"[STORAGE GUARD] Warning: Failed to query Turso PRAGMA: {e}")
            return
    else:
        target_db = db_file or "eod_tfex.db"
        if not os.path.exists(target_db):
            return
        try:
            conn = sqlite3.connect(target_db)
            cur = conn.cursor()
            cur.execute("PRAGMA page_count;")
            page_count = cur.fetchone()[0]
            cur.execute("PRAGMA page_size;")
            page_size = cur.fetchone()[0]
            conn.close()
        except Exception as e:
            print(f"[STORAGE GUARD] Warning: Failed to query local SQLite PRAGMA: {e}")
            return

    db_bytes = page_count * page_size
    db_mb = db_bytes / (1024 * 1024)
    usage_pct = (db_mb / MAX_FREE_QUOTA_MB) * 100
    status_label = "Healthy" if db_mb < TRIGGER_THRESHOLD_MB else "Action Required (> 80%)"

    print("=" * 70)
    print("STORAGE MANAGEMENT & RETENTION AUDIT")
    print("=" * 70)
    print(f"[STORAGE GUARD] Current DB Size: {db_mb:.2f} MB / {MAX_FREE_QUOTA_MB:.0f} MB ({usage_pct:.2f}%) - {status_label}")

    if db_mb < TRIGGER_THRESHOLD_MB:
        print("[STORAGE GUARD] Storage within healthy limit (< 80%). No retention purging needed.")
        return

    # Trigger threshold exceeded!
    print(f"[STORAGE GUARD] WARNING: DB Size ({db_mb:.2f} MB) exceeds 80% threshold ({TRIGGER_THRESHOLD_MB:.0f} MB).")
    print("[STORAGE GUARD] Evaluating FIFO Intraday Retention for spot, futures, and options snapshots...")

    oldest_dates = []

    if is_turso:
        reqs = []
        for tbl in INTRADAY_TABLES:
            reqs.append({"type": "execute", "stmt": {"sql": f"SELECT MIN(captured_at_bkk) FROM {tbl};"}})
        reqs.append({"type": "close"})

        try:
            r = requests.post(pipeline_url, headers=headers, json={"requests": reqs}, timeout=20)
            if r.status_code == 200:
                res = r.json()
                for idx, tbl in enumerate(INTRADAY_TABLES):
                    result_item = res["results"][idx]
                    if result_item.get("type") == "ok":
                        rows = result_item["response"]["result"]["rows"]
                        if rows and rows[0][0]["type"] != "null":
                            val = rows[0][0]["value"]
                            if val:
                                oldest_dates.append((val[:10], tbl))
        except Exception as e:
            print(f"[STORAGE GUARD] Error checking intraday timestamps: {e}")
            return
    else:
        try:
            conn = sqlite3.connect(target_db)
            cur = conn.cursor()
            for tbl in INTRADAY_TABLES:
                cur.execute(f"SELECT MIN(captured_at_bkk) FROM {tbl};")
                row = cur.fetchone()
                if row and row[0]:
                    oldest_dates.append((str(row[0])[:10], tbl))
            conn.close()
        except Exception as e:
            print(f"[STORAGE GUARD] Error checking local intraday timestamps: {e}")
            return

    if not oldest_dates:
        print("[STORAGE GUARD] Intraday snapshot tables are empty or uninitialized. Skipping.")
        return

    oldest_dates.sort(key=lambda x: x[0])
    oldest_date_str = oldest_dates[0][0]

    try:
        oldest_date = datetime.datetime.strptime(oldest_date_str, "%Y-%m-%d").date()
    except Exception as e:
        print(f"[STORAGE GUARD] Error parsing oldest date '{oldest_date_str}': {e}")
        return

    age_days = (current_date - oldest_date).days

    # Safety Lock: MUST be at least 365 days old
    if age_days < MIN_RETENTION_DAYS:
        print(f"[STORAGE GUARD] SAFETY LOCK ENGAGED: Oldest intraday record ({oldest_date_str}) is only {age_days} days old (< {MIN_RETENTION_DAYS} days).")
        print("[STORAGE GUARD] All intraday data within 1 year is strictly protected. Zero records deleted.")
        return

    # FIFO Pruning: Purge 7 days from oldest date
    cutoff_date = oldest_date + datetime.timedelta(days=PRUNE_CHUNK_DAYS)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d 23:59:59")
    print(f"[STORAGE GUARD] FIFO Pruning: Purging 7 days of historical intraday data (from {oldest_date_str} up to {cutoff_date})...")

    if is_turso:
        purge_reqs = []
        for tbl in INTRADAY_TABLES:
            purge_reqs.append({
                "type": "execute",
                "stmt": {
                    "sql": f"DELETE FROM {tbl} WHERE captured_at_bkk <= ?;",
                    "args": [{"type": "text", "value": cutoff_str}]
                }
            })
        purge_reqs.append({"type": "close"})

        try:
            r = requests.post(pipeline_url, headers=headers, json={"requests": purge_reqs}, timeout=30)
            if r.status_code == 200:
                print(f"[STORAGE GUARD] Successfully purged intraday records up to {cutoff_str}.")
        except Exception as e:
            print(f"[STORAGE GUARD] Error during purge: {e}")
            return

        check_reqs = [
            {"type": "execute", "stmt": {"sql": "PRAGMA page_count;"}},
            {"type": "execute", "stmt": {"sql": "PRAGMA page_size;"}},
            {"type": "close"}
        ]
        try:
            r = requests.post(pipeline_url, headers=headers, json={"requests": check_reqs}, timeout=20)
            if r.status_code == 200:
                res = r.json()
                new_pc = int(res["results"][0]["response"]["result"]["rows"][0][0]["value"])
                new_ps = int(res["results"][1]["response"]["result"]["rows"][0][0]["value"])
                new_mb = (new_pc * new_ps) / (1024 * 1024)
                print(f"[STORAGE GUARD] Post-cleanup DB Size: {new_mb:.2f} MB (Reclaimed: {db_mb - new_mb:.2f} MB)")
        except Exception:
            pass
    else:
        try:
            conn = sqlite3.connect(target_db)
            cur = conn.cursor()
            for tbl in INTRADAY_TABLES:
                cur.execute(f"DELETE FROM {tbl} WHERE captured_at_bkk <= ?;", (cutoff_str,))
            conn.commit()
            try:
                cur.execute("VACUUM;")
            except Exception:
                pass
            cur.execute("PRAGMA page_count;")
            new_pc = cur.fetchone()[0]
            cur.execute("PRAGMA page_size;")
            new_ps = cur.fetchone()[0]
            conn.close()
            new_mb = (new_pc * new_ps) / (1024 * 1024)
            print(f"[STORAGE GUARD] Post-cleanup DB Size: {new_mb:.2f} MB (Reclaimed: {db_mb - new_mb:.2f} MB)")
        except Exception as e:
            print(f"[STORAGE GUARD] Error during local purge/vacuum: {e}")

# ==============================================================================
# TELEGRAM EOD COMPLETION NOTIFIER
# ==============================================================================

def send_eod_telegram_alert(
    bot_token: str,
    chat_id: str,
    pipeline_url: str = None,
    auth_token: str = None,
    db_file: str = None,
    force_notify: bool = False
) -> bool:
    """
    Sends EOD data collection summary alert to Telegram on Mon-Fri morning (05:30 - 07:30 ICT).
    Checks eod_telegram_log to avoid duplicate alerts for the same trade_date.
    """
    now_bkk = datetime.datetime.now(BKK_TZ)
    now_bkk_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S ICT")
    is_turso = bool(pipeline_url and auth_token)

    print("=" * 70)
    print("TELEGRAM EOD COMPLETION NOTIFIER")
    print("=" * 70)

    # 1. Check Day-of-Week (Mon-Fri only)
    if not force_notify:
        if now_bkk.weekday() not in (0, 1, 2, 3, 4):
            weekday_name = now_bkk.strftime("%A")
            print(f"[TELEGRAM] Skipped: Today is {weekday_name} (weekend). Alerts sent Mon-Fri only.")
            return False

        # 2. Check Time Window (05:30 - 07:30 ICT)
        curr_time = now_bkk.time()
        start_w = datetime.time(5, 30)
        end_w = datetime.time(7, 30)
        if not (start_w <= curr_time <= end_w):
            print(f"[TELEGRAM] Skipped: Current time {curr_time.strftime('%H:%M')} is outside the 05:30-07:30 notification window.")
            return False
    else:
        print("[TELEGRAM] --notify flag detected. Bypassing day-of-week and time-window checks.")

    # 3. Retrieve Latest EOD Status & Check Duplicate Log
    latest_date = None
    already_notified = False
    prev_notified_at = None
    fut_cnt = 0
    opt_cnt = 0
    inv_cnt = 0
    mkt_cnt = 0
    thor_rate = None
    thor_1m = None
    thor_3m = None
    thor_6m = None
    thor_date = None
    size_mb = 0.0

    if is_turso:
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json"
        }
        reqs1 = [
            {"type": "execute", "stmt": {"sql": "SELECT MAX(trade_date) FROM eod_futures_series;"}},
            {"type": "execute", "stmt": {"sql": "SELECT trade_date, thor_rate, thor_1m_avg, thor_3m_avg, thor_6m_avg FROM eod_thor_rates ORDER BY trade_date DESC LIMIT 1;"}},
            {"type": "execute", "stmt": {"sql": "PRAGMA page_count;"}},
            {"type": "execute", "stmt": {"sql": "PRAGMA page_size;"}},
            {"type": "close"}
        ]
        try:
            r1 = requests.post(pipeline_url, headers=headers, json={"requests": reqs1}, timeout=20)
            if r1.status_code == 200:
                res1 = r1.json()
                rows0 = res1["results"][0]["response"]["result"]["rows"]
                if rows0 and rows0[0][0]["type"] != "null":
                    latest_date = rows0[0][0]["value"]

                rows1 = res1["results"][1]["response"]["result"]["rows"]
                if rows1:
                    r_val = rows1[0]
                    thor_date = r_val[0]["value"]
                    thor_rate = clean_float(r_val[1]["value"]) if len(r_val) > 1 else None
                    thor_1m = clean_float(r_val[2]["value"]) if len(r_val) > 2 else None
                    thor_3m = clean_float(r_val[3]["value"]) if len(r_val) > 3 else None
                    thor_6m = clean_float(r_val[4]["value"]) if len(r_val) > 4 else None

                pc = int(res1["results"][2]["response"]["result"]["rows"][0][0]["value"])
                ps = int(res1["results"][3]["response"]["result"]["rows"][0][0]["value"])
                size_mb = (pc * ps) / (1024 * 1024)
        except Exception as e:
            print(f"[TELEGRAM] Error retrieving Turso status: {e}")
            return False

        if not latest_date:
            print("[TELEGRAM] No data found in eod_futures_series. Skipping notification.")
            return False

        reqs2 = [
            {"type": "execute", "stmt": {"sql": "SELECT notified_at_bkk FROM eod_telegram_log WHERE trade_date = ?;", "args": [{"type": "text", "value": latest_date}]}},
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM eod_futures_series WHERE trade_date = ?;", "args": [{"type": "text", "value": latest_date}]}},
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM eod_options_series WHERE trade_date = ?;", "args": [{"type": "text", "value": latest_date}]}},
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM eod_investor_types WHERE trade_date = ?;", "args": [{"type": "text", "value": latest_date}]}},
            {"type": "execute", "stmt": {"sql": "SELECT COUNT(*) FROM eod_market_stats WHERE trade_date = ?;", "args": [{"type": "text", "value": latest_date}]}},
            {"type": "close"}
        ]
        try:
            r2 = requests.post(pipeline_url, headers=headers, json={"requests": reqs2}, timeout=20)
            if r2.status_code == 200:
                res2 = r2.json()
                log_rows = res2["results"][0]["response"]["result"]["rows"]
                if log_rows and len(log_rows) > 0:
                    already_notified = True
                    prev_notified_at = log_rows[0][0]["value"]
                fut_cnt = int(res2["results"][1]["response"]["result"]["rows"][0][0]["value"])
                opt_cnt = int(res2["results"][2]["response"]["result"]["rows"][0][0]["value"])
                inv_cnt = int(res2["results"][3]["response"]["result"]["rows"][0][0]["value"])
                mkt_cnt = int(res2["results"][4]["response"]["result"]["rows"][0][0]["value"])
        except Exception as e:
            print(f"[TELEGRAM] Error querying row counts: {e}")
            return False
    else:
        target_db = db_file or "eod_tfex.db"
        if not os.path.exists(target_db):
            print(f"[TELEGRAM] Local database {target_db} not found.")
            return False
        try:
            conn = sqlite3.connect(target_db)
            cur = conn.cursor()
            cur.execute("SELECT MAX(trade_date) FROM eod_futures_series;")
            row = cur.fetchone()
            latest_date = row[0] if row else None
            if not latest_date:
                conn.close()
                return False

            cur.execute("SELECT notified_at_bkk FROM eod_telegram_log WHERE trade_date = ?;", (latest_date,))
            log_row = cur.fetchone()
            if log_row:
                already_notified = True
                prev_notified_at = log_row[0]

            cur.execute("SELECT COUNT(*) FROM eod_futures_series WHERE trade_date = ?;", (latest_date,))
            fut_cnt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM eod_options_series WHERE trade_date = ?;", (latest_date,))
            opt_cnt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM eod_investor_types WHERE trade_date = ?;", (latest_date,))
            inv_cnt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM eod_market_stats WHERE trade_date = ?;", (latest_date,))
            mkt_cnt = cur.fetchone()[0]

            cur.execute("SELECT trade_date, thor_rate, thor_1m_avg, thor_3m_avg, thor_6m_avg FROM eod_thor_rates ORDER BY trade_date DESC LIMIT 1;")
            t_row = cur.fetchone()
            if t_row:
                thor_date = t_row[0]
                thor_rate = clean_float(t_row[1]) if len(t_row) > 1 else None
                thor_1m = clean_float(t_row[2]) if len(t_row) > 2 else None
                thor_3m = clean_float(t_row[3]) if len(t_row) > 3 else None
                thor_6m = clean_float(t_row[4]) if len(t_row) > 4 else None

            cur.execute("PRAGMA page_count;")
            pc = cur.fetchone()[0]
            cur.execute("PRAGMA page_size;")
            ps = cur.fetchone()[0]
            size_mb = (pc * ps) / (1024 * 1024)
            conn.close()
        except Exception as e:
            print(f"[TELEGRAM] Error querying local SQLite: {e}")
            return False

    # Check (c) Duplicate notification guard
    if already_notified and not force_notify:
        print(f"[TELEGRAM] Skipped: Trade date {latest_date} was already notified on {prev_notified_at}.")
        return False

    # 4. Format Telegram HTML Message
    pct = (size_mb / MAX_FREE_QUOTA_MB) * 100
    thor_text = f"<code>{thor_rate:.5f}%</code>" if thor_rate is not None else "N/A"
    thor_1m_text = f"<code>{thor_1m:.5f}%</code>" if thor_1m is not None else "N/A"
    thor_3m_text = f"<code>{thor_3m:.5f}%</code>" if thor_3m is not None else "N/A"
    thor_6m_text = f"<code>{thor_6m:.5f}%</code>" if thor_6m is not None else "N/A"
    thor_disp_date = thor_date or latest_date

    message = (
        f"📊 <b>TFEX & BOT EOD Collection Complete</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>EOD Data สำหรับวันเทรด {latest_date} ถูกดึงลง Turso เรียบร้อยแล้ว</b>\n\n"
        f"<b>📦 รายการข้อมูลที่จัดเก็บ ({latest_date}):</b>\n"
        f"• <b>Futures Series</b>: <code>{fut_cnt}</code> สัญญา (Active Series)\n"
        f"• <b>Options Series</b>: <code>{opt_cnt}</code> สัญญา (All Strikes C/P)\n"
        f"• <b>Investor Types</b>: <code>{inv_cnt}</code> กลุ่ม (Futures, Calls, Puts)\n"
        f"• <b>Market Stats</b>: <code>{mkt_cnt}</code> หมวดสินค้า\n\n"
        f"<b>📈 ดอกเบี้ยอ้างอิง BOT THOR ({thor_disp_date}):</b>\n"
        f"• <b>THOR Overnight</b>: {thor_text}\n"
        f"• <b>THOR 1M Average</b>: {thor_1m_text}\n"
        f"• <b>THOR 3M Average</b>: {thor_3m_text}\n"
        f"• <b>THOR 6M Average</b>: {thor_6m_text}\n\n"
        f"<b>💾 Turso Database Status:</b>\n"
        f"• <b>Storage Usage</b>: <code>{size_mb:.2f} MB / {MAX_FREE_QUOTA_MB:.0f} MB ({pct:.2f}%)</code> - Healthy\n"
        f"• <b>Notified At</b>: <code>{now_bkk_str}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )

    # 5. Dispatch Message via Telegram Bot API
    tg_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        resp = requests.post(tg_url, json=payload, timeout=15)
        if resp.status_code != 200:
            print(f"[TELEGRAM] Warning: Telegram API failed ({resp.status_code}): {resp.text}")
            return False
        print(f"[TELEGRAM] Notification dispatched successfully to chat_id {chat_id} for trade date {latest_date}.")
    except Exception as e:
        print(f"[TELEGRAM] Error sending Telegram message: {e}")
        return False

    # 6. Record Notification in eod_telegram_log
    snippet = f"EOD {latest_date} completed (F:{fut_cnt}, O:{opt_cnt}, I:{inv_cnt})"
    sql_log = "INSERT OR REPLACE INTO eod_telegram_log (trade_date, notified_at_bkk, message_snippet) VALUES (?, ?, ?);"

    if is_turso:
        log_stmt = [{
            "type": "execute",
            "stmt": {
                "sql": sql_log,
                "args": [
                    {"type": "text", "value": latest_date},
                    {"type": "text", "value": now_bkk_str},
                    {"type": "text", "value": snippet}
                ]
            }
        }, {"type": "close"}]
        try:
            requests.post(pipeline_url, headers=headers, json={"requests": log_stmt}, timeout=15)
            print(f"[TELEGRAM] Recorded trade_date {latest_date} in eod_telegram_log.")
        except Exception as e:
            print(f"[TELEGRAM] Warning: Failed to record notification log in Turso: {e}")
    else:
        try:
            conn = sqlite3.connect(target_db)
            cur = conn.cursor()
            cur.execute(sql_log, (latest_date, now_bkk_str, snippet))
            conn.commit()
            conn.close()
            print(f"[TELEGRAM] Recorded trade_date {latest_date} in eod_telegram_log.")
        except Exception as e:
            print(f"[TELEGRAM] Warning: Failed to record notification log in local SQLite: {e}")

    return True

# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="TFEX & BOT Stateless EOD Data Collector")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force execution regardless of weekend or official SET holidays"
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="Force send Telegram completion alert regardless of time window or prior notification"
    )
    parser.add_argument(
        "--local-db",
        type=str,
        default=None,
        help="Optional local SQLite database file path (e.g. eod_tfex.db)"
    )
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()
    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")

    now_bkk = datetime.datetime.now(BKK_TZ)
    print("=" * 70)
    print(f"TFEX & BOT STATLESS EOD DATA COLLECTOR")
    print(f"Execution Time : {now_bkk.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 70)

    # 1. Holiday & Weekend Guard
    is_holiday, reason = is_holiday_or_weekend(now_bkk)
    if is_holiday:
        print(f"[GUARD] {reason}.")
        if not (args.force or args.notify):
            print("[GUARD] Skipping execution. (Use --force or --notify to run anyway).")
            sys.exit(0)
        else:
            print("[GUARD] Override flag detected (--force / --notify). Proceeding with execution.")
    else:
        print(f"[GUARD] Check passed: {reason}.")

    # 2. Determine Active Quarterly Series
    active_series = get_active_quarterly_series(now_bkk)
    print(f"[SERIES] Active Quarterly Series (H, M, U, Z): {active_series}")

    # 3. Data Collection Pipeline
    start_time = time.time()
    tfex_session = init_tfex_session()

    inv_types = fetch_investor_types(tfex_session)
    mkt_stats = fetch_market_stats(tfex_session)
    fut_series = fetch_futures_series(tfex_session, active_series)
    opt_series = fetch_options_series(tfex_session, active_series)
    thor_rates = fetch_thor_rates()

    elapsed_collect = time.time() - start_time
    print(f"[COLLECT] Data extraction completed in {elapsed_collect:.2f} seconds.")

    # 4. Generate Idempotent Upsert Statements
    statements = build_upsert_statements(
        inv_types=inv_types,
        mkt_stats=mkt_stats,
        fut_series=fut_series,
        opt_series=opt_series,
        thor_rates=thor_rates
    )
    print(f"[DATABASE] Prepared {len(statements)} total statements (including schema DDL).")

    # 5. Persist to Turso Cloud SQLite or Local Fallback
    if turso_url and turso_token:
        pipeline_url = normalize_turso_url(turso_url)
        print(f"[DATABASE] Target: Turso Cloud SQLite ({pipeline_url})")
        execute_turso_pipeline(pipeline_url, turso_token, statements)
    else:
        db_file = args.local_db or "eod_tfex.db"
        print(f"[DATABASE] No Turso credentials found. Using local SQLite database: {db_file}")
        execute_sqlite_local(db_file, statements)

    # 5.1 EOD Black-76 Full Greeks Calculation Pipeline
    try:
        from greeks_engine import run_eod_greeks_pipeline
        if turso_url and turso_token:
            run_eod_greeks_pipeline(pipeline_url=pipeline_url, auth_token=turso_token)
        else:
            run_eod_greeks_pipeline(db_file=db_file)
    except Exception as e:
        print(f"[GREEKS] Error running EOD Greeks pipeline: {e}")

    # Summary Audit
    print("=" * 70)
    print("EOD DATA COLLECTION SUMMARY AUDIT")
    print("=" * 70)
    print(f"1. Investor Types   : {len(inv_types):>5} rows (trade_date: {inv_types[0]['trade_date'] if inv_types else 'N/A'})")
    print(f"2. Market Stats     : {len(mkt_stats):>5} rows")
    print(f"3. Futures Series   : {len(fut_series):>5} rows (series: {', '.join(active_series)})")
    print(f"4. Options Series   : {len(opt_series):>5} rows")
    print(f"5. BOT THOR Rates   : {len(thor_rates):>5} rows (trade_date: {thor_rates[0]['trade_date'] if thor_rates else 'N/A'})")
    print(f"Total Execution Time: {time.time() - start_time:.2f} seconds.")
    # 6. Automated FIFO Intraday Retention & Storage Guard
    if turso_url and turso_token:
        pipeline_url = normalize_turso_url(turso_url)
        run_fifo_retention_guard(pipeline_url=pipeline_url, auth_token=turso_token)
    else:
        db_file = args.local_db or "eod_tfex.db"
        run_fifo_retention_guard(db_file=db_file)

    # 7. Automated Telegram EOD Completion Alert
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if bot_token and chat_id:
        if turso_url and turso_token:
            pipeline_url = normalize_turso_url(turso_url)
            send_eod_telegram_alert(
                bot_token=bot_token,
                chat_id=chat_id,
                pipeline_url=pipeline_url,
                auth_token=turso_token,
                force_notify=args.notify
            )
        else:
            db_file = args.local_db or "eod_tfex.db"
            send_eod_telegram_alert(
                bot_token=bot_token,
                chat_id=chat_id,
                db_file=db_file,
                force_notify=args.notify
            )
    else:
        print("[TELEGRAM] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not configured. Skipping alert.")

    print("=" * 70)
    print("SUCCESS: Stateless EOD Data Collection completed idempotently.")

if __name__ == "__main__":
    main()
