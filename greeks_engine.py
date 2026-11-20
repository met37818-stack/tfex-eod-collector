"""
Black-76 Full Greeks Engine (greeks_engine.py)
==============================================
Calculates Black-76 First-Order and Second-Order Greeks for TFEX SET50 Options:
- First-Order: Implied Volatility (IV), Delta, Gamma, Theta (per day), Vega (per 1% IV)
- Second-Order: Vanna (dDelta / dSigma), Charm (dDelta / dt per day)

Strict Fail-Loud / No Silent Defaults:
- If market price is missing, <= intrinsic, or IV cannot be computed:
  Greeks are recorded as NULL.
- Tracks and reports invalid / illiquid quotes explicitly.
- Persists into Turso Cloud SQLite / Local SQLite table `eod_options_greeks`.
"""

import os
import sys
import math
import argparse
import datetime
from datetime import date, time
from typing import Dict, List, Tuple, Any, Optional

from calendar_utils import get_penultimate_business_day, get_series_expiry_date

# Thailand Timezone
try:
    import pytz
    BKK_TZ = pytz.timezone("Asia/Bangkok")
except ImportError:
    BKK_TZ = datetime.timezone(datetime.timedelta(hours=7))

# SET_HOLIDAYS lives in calendar_utils.py — see the module docstring there for
# why a single shared copy matters (this file used to carry an incomplete list,
# which gave S50Z27 a different expiry than the collector computed).

# ==============================================================================
# 1. MATHEMATICAL FORMULAS (PURE PYTHON STANDARD LIBRARY)
# ==============================================================================

def norm_cdf(x: float) -> float:
    """Standard normal cumulative distribution function N(x)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def norm_pdf(x: float) -> float:
    """Standard normal probability density function phi(x)."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def black76_price(F: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
    """
    Black-76 European futures option pricing formula.
    F: Underlying futures price
    K: Strike price
    T: Time to expiration in years
    r: Annualized risk-free interest rate
    sigma: Annualized volatility
    option_type: 'C' (Call) or 'P' (Put)
    """
    if sigma <= 0.0 or T <= 0.0:
        disc = math.exp(-r * T)
        return max(0.0, disc * ((F - K) if option_type == 'C' else (K - F)))

    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    disc = math.exp(-r * T)

    if option_type == 'C':
        return disc * (F * norm_cdf(d1) - K * norm_cdf(d2))
    else:
        return disc * (K * norm_cdf(-d2) - F * norm_cdf(-d1))

def calculate_implied_vol(price: float, F: float, K: float, T: float, r: float, option_type: str) -> Optional[float]:
    """
    Solves for Black-76 Implied Volatility via Brent's method / Bisection.
    Strict Validation: Returns None if price is <= intrinsic, <= 0, or unresolvable.
    """
    if price is None or price <= 0.0 or T <= 0.0 or F <= 0.0 or K <= 0.0:
        return None

    disc = math.exp(-r * T)
    intrinsic = max(0.0, disc * ((F - K) if option_type == 'C' else (K - F)))
    if price <= intrinsic + 1e-4:
        return None

    # Cap Implied Volatility search at 1.50 (150%) to prevent deep OTM/ITM numerical blowup
    MAX_IV = 1.50
    p_high = black76_price(F, K, T, r, MAX_IV, option_type)
    if price >= p_high:
        return None

    low = 0.0001
    high = MAX_IV
    for _ in range(80):
        mid = 0.5 * (low + high)
        p_mid = black76_price(F, K, T, r, mid, option_type)
        diff = p_mid - price
        if abs(diff) < 1e-5:
            return mid if mid <= MAX_IV else None
        if diff < 0.0:
            low = mid
        else:
            high = mid
    res = 0.5 * (low + high)
    return res if (res is not None and res <= MAX_IV) else None

def calculate_black76_full_greeks(
    F: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str
) -> Dict[str, float]:
    """
    Computes First & Second Order Black-76 Greeks:
    - Delta: dV / dF
    - Gamma: d^2V / dF^2
    - Theta (per day): dV / dt (/ 365)
    - Vega (per 1% IV): (dV / dSigma) * 0.01
    - Vanna: dDelta / dSigma
    - Charm (per day): dDelta / dt (/ 365)
    """
    if sigma <= 0.0 or T <= 0.0 or F <= 0.0 or K <= 0.0:
        raise ValueError("Invalid parameters for Greeks calculation")

    sqrt_T = math.sqrt(T)
    d1 = (math.log(F / K) + 0.5 * sigma * sigma * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    disc = math.exp(-r * T)
    pdf_d1 = norm_pdf(d1)

    # First-Order Greeks
    gamma = (disc * pdf_d1) / (F * sigma * sqrt_T)
    vega_1pct = (F * disc * pdf_d1 * sqrt_T) * 0.01

    if option_type == 'C':
        delta = disc * norm_cdf(d1)
        theta_raw = - (F * disc * pdf_d1 * sigma) / (2.0 * sqrt_T) - r * disc * (F * norm_cdf(d1) - K * norm_cdf(d2))
        theta_1d = theta_raw / 365.0
        charm_raw = r * disc * norm_cdf(d1) + disc * pdf_d1 * (d2 / (2.0 * T))
        charm_1d = charm_raw / 365.0
    else:
        delta = -disc * norm_cdf(-d1)
        theta_raw = - (F * disc * pdf_d1 * sigma) / (2.0 * sqrt_T) - r * disc * (K * norm_cdf(-d2) - F * norm_cdf(-d1))
        theta_1d = theta_raw / 365.0
        charm_raw = -r * disc * norm_cdf(-d1) + disc * pdf_d1 * (d2 / (2.0 * T))
        charm_1d = charm_raw / 365.0

    # Dampen / cap Charm when DTE <= 2 to prevent near-expiry 1/T asymptotic explosion
    if T <= (2.0 / 365.0) or math.isnan(charm_1d) or math.isinf(charm_1d):
        charm_1d = max(-5.0, min(5.0, charm_1d)) if not (math.isnan(charm_1d) or math.isinf(charm_1d)) else 0.0

    # Second-Order Greek: Vanna (Identical for Call and Put)
    vanna = -disc * pdf_d1 * (d2 / sigma)

    return {
        "delta": float(delta),
        "gamma": float(gamma),
        "theta": float(theta_1d),
        "vega": float(vega_1pct),
        "vanna": float(vanna),
        "charm": float(charm_1d)
    }

# ==============================================================================
# 2. CALENDAR & EXPIRY UTILITIES
# ==============================================================================
# SET_HOLIDAYS, get_penultimate_business_day() and get_series_expiry_date() are
# imported from calendar_utils.py (one shared copy with eod_collector.py).

# ==============================================================================
# 3. SCHEMA DDL FOR TURSO / SQLITE
# ==============================================================================

CREATE_GREEKS_TABLE_SQL = """
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

# ==============================================================================
# 4. GREEKS PROCESSING ENGINE
# ==============================================================================

# BOT publishes THOR in percent (1.25 == 1.25% p.a.). Never infer the unit from
# the magnitude of the number: a genuine sub-0.5% rate (or a 6M average in a
# steep cutting cycle) used to be read as a decimal and inflated x100, silently
# corrupting IV and every downstream greek.
THOR_MAX_PCT = 20.0


def select_dynamic_thor_rate(
    thor_record: Dict[str, Optional[float]],
    dte: int
) -> Tuple[float, str]:
    """
    Selects dynamic THOR rate matching the option's DTE (Days to Expiration).
    
    Tenor Selection Rules:
    - dte <= 30 days:
        Prefer 1M Compounded ('1m')
        Fallback: Overnight ('overnight')
    - 30 < dte <= 90 days:
        Prefer 3M Compounded ('3m')
        Fallback: 1M Compounded ('1m') -> Overnight ('overnight')
    - 90 < dte <= 180 days:
        Prefer 6M Compounded ('6m')
        Fallback: 3M Compounded ('3m') -> 1M Compounded -> Overnight
    - dte > 180 days:
        Prefer 6M Compounded ('6m')
        Fallback: 3M Compounded -> Overnight
    """
    def _to_decimal_rate(pct: Optional[float], tenor: str) -> Optional[float]:
        """BOT publishes THOR in percent (1.25 == 1.25% p.a.). Always divide by 100."""
        if pct is None or pct <= 0:
            return None
        if pct > THOR_MAX_PCT:
            raise ValueError(
                f"THOR {tenor} out of range: {pct} (expected percent, <= {THOR_MAX_PCT})"
            )
        return pct / 100.0

    r_on = _to_decimal_rate(thor_record.get("overnight"), "overnight")
    r_1m = _to_decimal_rate(thor_record.get("1m"), "1m")
    r_3m = _to_decimal_rate(thor_record.get("3m"), "3m")
    r_6m = _to_decimal_rate(thor_record.get("6m"), "6m")

    if dte <= 30:
        candidates = [("1M", r_1m), ("ON", r_on), ("3M", r_3m), ("6M", r_6m)]
    elif dte <= 90:
        candidates = [("3M", r_3m), ("1M", r_1m), ("ON", r_on), ("6M", r_6m)]
    elif dte <= 180:
        candidates = [("6M", r_6m), ("3M", r_3m), ("1M", r_1m), ("ON", r_on)]
    else:
        candidates = [("6M", r_6m), ("3M", r_3m), ("ON", r_on), ("1M", r_1m)]

    for label, rate in candidates:
        if rate is not None and rate > 0:
            return rate, label

    raise ValueError(f"No valid THOR rate found in record: {thor_record}")


def compute_greeks_records_for_date(
    trade_date: str,
    options_rows: List[Dict[str, Any]],
    futures_dict: Dict[str, float],
    thor_rate: Any,
    now_bkk_str: str
) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Processes options records for a single trade_date.
    Returns: (greeks_records, valid_iv_count, invalid_iv_count)
    """
    trade_date_obj = datetime.datetime.strptime(trade_date, "%Y-%m-%d").date()
    greeks_records = []
    valid_count = 0
    invalid_count = 0

    for opt in options_rows:
        symbol = str(opt["symbol"])
        series = str(opt["series"])
        opt_type = str(opt["option_type"]).upper()
        strike = float(opt["strike"])
        oi = opt.get("oi")

        # Resolve underlying futures symbol, e.g. S50Z26
        underlying = series if series.startswith("S50") else f"S50{series}"
        f_price = futures_dict.get(underlying)

        if not f_price or f_price <= 0:
            # Underlying price missing -> fail loud by marking as invalid
            greeks_records.append({
                "trade_date": trade_date, "symbol": symbol, "series": series,
                "option_type": opt_type, "strike": strike, "underlying_futures": underlying,
                "dte": 0, "thor_rate": thor_rate, "iv": None, "delta": None,
                "gamma": None, "theta": None, "vega": None, "vanna": None,
                "charm": None, "open_interest": oi, "updated_at_bkk": now_bkk_str
            })
            invalid_count += 1
            continue

        # Expiry & DTE
        expiry_date = get_series_expiry_date(series)
        dte = max(0, (expiry_date - trade_date_obj).days)
        T_years = max(dte, 0.5) / 365.0

        # Dynamic contract-specific THOR rate
        if isinstance(thor_rate, dict):
            contract_thor, _ = select_dynamic_thor_rate(thor_rate, dte)
        elif isinstance(thor_rate, (int, float)) and thor_rate > 0:
            contract_thor = float(thor_rate)
        else:
            raise ValueError(f"Invalid THOR rate provided: {thor_rate}")

        # Option pricing reference: settlement_price ONLY (Strict Fail-Loud / No Close fallback)
        opt_price = opt.get("settlement_price")

        iv = None
        if opt_price and opt_price > 0:
            iv = calculate_implied_vol(opt_price, f_price, strike, T_years, contract_thor, opt_type)

        if iv is not None and iv > 0.0001:
            try:
                g = calculate_black76_full_greeks(f_price, strike, T_years, contract_thor, iv, opt_type)
                greeks_records.append({
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "series": series,
                    "option_type": opt_type,
                    "strike": strike,
                    "underlying_futures": underlying,
                    "dte": dte,
                    "thor_rate": round(contract_thor, 6),
                    "iv": round(iv, 6),
                    "delta": round(g["delta"], 6),
                    "gamma": round(g["gamma"], 6),
                    "theta": round(g["theta"], 6),
                    "vega": round(g["vega"], 6),
                    "vanna": round(g["vanna"], 6),
                    "charm": round(g["charm"], 6),
                    "open_interest": oi,
                    "updated_at_bkk": now_bkk_str
                })
                valid_count += 1
            except Exception:
                greeks_records.append({
                    "trade_date": trade_date, "symbol": symbol, "series": series,
                    "option_type": opt_type, "strike": strike, "underlying_futures": underlying,
                    "dte": dte, "thor_rate": round(contract_thor, 6), "iv": None, "delta": None,
                    "gamma": None, "theta": None, "vega": None, "vanna": None,
                    "charm": None, "open_interest": oi, "updated_at_bkk": now_bkk_str
                })
                invalid_count += 1
        else:
            # Strict Fail-Loud: record NULL for uncomputable/illiquid quotes
            greeks_records.append({
                "trade_date": trade_date,
                "symbol": symbol,
                "series": series,
                "option_type": opt_type,
                "strike": strike,
                "underlying_futures": underlying,
                "dte": dte,
                "thor_rate": round(contract_thor, 6),
                "iv": None,
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
                "vanna": None,
                "charm": None,
                "open_interest": oi,
                "updated_at_bkk": now_bkk_str
            })
            invalid_count += 1

    return greeks_records, valid_count, invalid_count

# ==============================================================================
# 5. PIPELINE EXECUTION FOR TURSO & LOCAL SQLITE
# ==============================================================================

def to_turso_arg(val: Any) -> Dict[str, Any]:
    if val is None:
        return {"type": "null"}
    if isinstance(val, int):
        return {"type": "integer", "value": val}
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return {"type": "null"}
        return {"type": "float", "value": val}
    return {"type": "text", "value": str(val)}

def run_eod_greeks_pipeline(
    pipeline_url: Optional[str] = None,
    auth_token: Optional[str] = None,
    db_file: Optional[str] = None,
    target_date: Optional[str] = None
) -> bool:
    """
    Main entry point for calculating and persisting EOD Options Greeks.
    Runs against Turso HTTP Pipeline or local SQLite.
    """
    import sqlite3
    import requests

    now_bkk = datetime.datetime.now(BKK_TZ)
    now_bkk_str = now_bkk.strftime("%Y-%m-%d %H:%M:%S")

    is_turso = bool(pipeline_url and auth_token)
    print("=" * 70)
    print("EOD BLACK-76 FULL GREEKS CALCULATION ENGINE")
    print(f"Timestamp: {now_bkk_str} | Mode: {'Turso Cloud' if is_turso else 'Local SQLite'}")
    print("=" * 70)

    # 1. Fetch available trade dates
    dates_to_process = []
    if is_turso:
        headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
        # Ensure schema table exists
        ensure_req = {"requests": [{"type": "execute", "stmt": {"sql": CREATE_GREEKS_TABLE_SQL}}, {"type": "close"}]}
        requests.post(pipeline_url, headers=headers, json=ensure_req, timeout=15)

        if target_date:
            dates_to_process = [target_date]
        else:
            sql_dt = "SELECT DISTINCT trade_date FROM eod_options_series ORDER BY trade_date DESC LIMIT 5;"
            r = requests.post(pipeline_url, headers=headers, json={"requests": [{"type": "execute", "stmt": {"sql": sql_dt}}, {"type": "close"}]}, timeout=15)
            if r.status_code == 200:
                rows = r.json().get("results", [])[0].get("response", {}).get("result", {}).get("rows", [])
                dates_to_process = [row[0].get("value") for row in rows if row and row[0].get("value")]
    else:
        target_db = db_file or "eod_tfex.db"
        if not os.path.exists(target_db):
            print(f"[GREEKS] Database file not found: {target_db}")
            return False
        conn = sqlite3.connect(target_db)
        cur = conn.cursor()
        cur.execute(CREATE_GREEKS_TABLE_SQL)
        conn.commit()

        if target_date:
            dates_to_process = [target_date]
        else:
            cur.execute("SELECT DISTINCT trade_date FROM eod_options_series ORDER BY trade_date DESC LIMIT 5;")
            dates_to_process = [r[0] for r in cur.fetchall() if r[0]]
        conn.close()

    if not dates_to_process:
        print("[GREEKS] No trade dates found in eod_options_series.")
        return False

    total_valid = 0
    total_invalid = 0

    for t_date in dates_to_process:
        print(f"\n[GREEKS] Processing Trade Date: {t_date}...")

        # 2. Query Data for this date
        options_rows = []
        futures_dict = {}
        thor_dict = {"overnight": None, "1m": None, "3m": None, "6m": None}

        if is_turso:
            sql_opt = "SELECT symbol, series, option_type, strike, settlement_price, close, oi FROM eod_options_series WHERE trade_date = ?;"
            sql_fut = "SELECT series, settlement_price, close FROM eod_futures_series WHERE trade_date = ?;"
            sql_thor = "SELECT thor_rate, thor_1m_avg, thor_3m_avg, thor_6m_avg FROM eod_thor_rates WHERE trade_date <= ? AND thor_rate IS NOT NULL AND thor_rate > 0 ORDER BY trade_date DESC LIMIT 1;"

            reqs = [
                {"type": "execute", "stmt": {"sql": sql_opt, "args": [{"type": "text", "value": t_date}]}},
                {"type": "execute", "stmt": {"sql": sql_fut, "args": [{"type": "text", "value": t_date}]}},
                {"type": "execute", "stmt": {"sql": sql_thor, "args": [{"type": "text", "value": t_date}]}},
                {"type": "close"}
            ]
            r = requests.post(pipeline_url, headers=headers, json={"requests": reqs}, timeout=20)
            if r.status_code != 200:
                print(f"[GREEKS] Failed to fetch data from Turso ({r.status_code}): {r.text}")
                continue
            res = r.json().get("results", [])
            # Parse options
            opt_raw = res[0].get("response", {}).get("result", {}).get("rows", [])
            for row in opt_raw:
                options_rows.append({
                    "symbol": row[0].get("value"),
                    "series": row[1].get("value"),
                    "option_type": row[2].get("value"),
                    "strike": float(row[3].get("value", 0.0)),
                    "settlement_price": float(row[4].get("value")) if row[4].get("type") in ("float", "integer") else None,
                    "close": float(row[5].get("value")) if row[5].get("type") in ("float", "integer") else None,
                    "oi": int(row[6].get("value")) if row[6].get("type") == "integer" else 0
                })
            # Parse futures
            fut_raw = res[1].get("response", {}).get("result", {}).get("rows", [])
            for row in fut_raw:
                f_ser = row[0].get("value")
                settle = float(row[1].get("value")) if row[1].get("type") in ("float", "integer") else None
                close_p = float(row[2].get("value")) if row[2].get("type") in ("float", "integer") else None
                ref_p = settle if settle and settle > 0 else close_p
                if f_ser and ref_p:
                    futures_dict[f_ser] = ref_p
                    if not f_ser.startswith("S50"):
                        futures_dict[f"S50{f_ser}"] = ref_p
            # Parse thor
            thor_raw = res[2].get("response", {}).get("result", {}).get("rows", [])
            if thor_raw and thor_raw[0]:
                r_thor = thor_raw[0]
                thor_dict["overnight"] = float(r_thor[0].get("value")) if len(r_thor) > 0 and r_thor[0].get("value") is not None else None
                thor_dict["1m"] = float(r_thor[1].get("value")) if len(r_thor) > 1 and r_thor[1].get("value") is not None else None
                thor_dict["3m"] = float(r_thor[2].get("value")) if len(r_thor) > 2 and r_thor[2].get("value") is not None else None
                thor_dict["6m"] = float(r_thor[3].get("value")) if len(r_thor) > 3 and r_thor[3].get("value") is not None else None
        else:
            conn = sqlite3.connect(target_db)
            cur = conn.cursor()
            cur.execute("SELECT symbol, series, option_type, strike, settlement_price, close, oi FROM eod_options_series WHERE trade_date = ?;", (t_date,))
            for row in cur.fetchall():
                options_rows.append({
                    "symbol": row[0], "series": row[1], "option_type": row[2],
                    "strike": float(row[3]), "settlement_price": row[4], "close": row[5], "oi": row[6]
                })
            cur.execute("SELECT series, settlement_price, close FROM eod_futures_series WHERE trade_date = ?;", (t_date,))
            for row in cur.fetchall():
                f_ser = row[0]
                ref_p = row[1] if row[1] and row[1] > 0 else row[2]
                if f_ser and ref_p:
                    futures_dict[f_ser] = ref_p
                    if not f_ser.startswith("S50"):
                        futures_dict[f"S50{f_ser}"] = ref_p
            cur.execute("SELECT thor_rate, thor_1m_avg, thor_3m_avg, thor_6m_avg FROM eod_thor_rates WHERE trade_date <= ? AND thor_rate IS NOT NULL AND thor_rate > 0 ORDER BY trade_date DESC LIMIT 1;", (t_date,))
            tr_row = cur.fetchone()
            if tr_row:
                thor_dict["overnight"] = float(tr_row[0]) if len(tr_row) > 0 and tr_row[0] is not None else None
                thor_dict["1m"] = float(tr_row[1]) if len(tr_row) > 1 and tr_row[1] is not None else None
                thor_dict["3m"] = float(tr_row[2]) if len(tr_row) > 2 and tr_row[2] is not None else None
                thor_dict["6m"] = float(tr_row[3]) if len(tr_row) > 3 and tr_row[3] is not None else None
            conn.close()

        if not any(v is not None and v > 0 for v in thor_dict.values()):
            raise ValueError(f"No valid THOR rate found in eod_thor_rates for {t_date}")

        if not options_rows:
            print(f"[GREEKS] No options rows found for {t_date}.")
            continue

        print(f"[GREEKS] Computing Greeks for {len(options_rows)} options | Futures Ref: {futures_dict} | Dynamic THOR Rates: {thor_dict}")
        greeks_recs, v_cnt, inv_cnt = compute_greeks_records_for_date(
            trade_date=t_date,
            options_rows=options_rows,
            futures_dict=futures_dict,
            thor_rate=thor_dict,
            now_bkk_str=now_bkk_str
        )
        total_valid += v_cnt
        total_invalid += inv_cnt

        print(f"[GREEKS] {t_date} Results: {v_cnt} Valid IV & Greeks, {inv_cnt} Invalid/Illiquid (Recorded as NULL).")

        # 3. Persist to Turso or SQLite
        sql_upsert = """
        INSERT OR REPLACE INTO eod_options_greeks (
            trade_date, symbol, series, option_type, strike, underlying_futures,
            dte, thor_rate, iv, delta, gamma, theta, vega, vanna, charm,
            open_interest, updated_at_bkk
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        if is_turso:
            # Batch pipeline into chunks of 100
            chunk_size = 100
            for i in range(0, len(greeks_recs), chunk_size):
                chunk = greeks_recs[i:i + chunk_size]
                chunk_reqs = []
                for r in chunk:
                    args = [
                        to_turso_arg(r["trade_date"]), to_turso_arg(r["symbol"]),
                        to_turso_arg(r["series"]), to_turso_arg(r["option_type"]),
                        to_turso_arg(r["strike"]), to_turso_arg(r["underlying_futures"]),
                        to_turso_arg(r["dte"]), to_turso_arg(r["thor_rate"]),
                        to_turso_arg(r["iv"]), to_turso_arg(r["delta"]),
                        to_turso_arg(r["gamma"]), to_turso_arg(r["theta"]),
                        to_turso_arg(r["vega"]), to_turso_arg(r["vanna"]),
                        to_turso_arg(r["charm"]), to_turso_arg(r["open_interest"]),
                        to_turso_arg(r["updated_at_bkk"])
                    ]
                    chunk_reqs.append({"type": "execute", "stmt": {"sql": sql_upsert, "args": args}})
                chunk_reqs.append({"type": "close"})
                resp = requests.post(pipeline_url, headers=headers, json={"requests": chunk_reqs}, timeout=25)
                if resp.status_code != 200:
                    print(f"[GREEKS] Warning: Turso batch failed ({resp.status_code}): {resp.text}")
            print(f"[GREEKS] Successfully persisted {len(greeks_recs)} records to Turso `eod_options_greeks`.")
        else:
            conn = sqlite3.connect(target_db)
            cur = conn.cursor()
            for r in greeks_recs:
                cur.execute(sql_upsert, (
                    r["trade_date"], r["symbol"], r["series"], r["option_type"],
                    r["strike"], r["underlying_futures"], r["dte"], r["thor_rate"],
                    r["iv"], r["delta"], r["gamma"], r["theta"], r["vega"],
                    r["vanna"], r["charm"], r["open_interest"], r["updated_at_bkk"]
                ))
            conn.commit()
            conn.close()
            print(f"[GREEKS] Successfully committed {len(greeks_recs)} records to local SQLite `eod_options_greeks`.")

    print("=" * 70)
    print(f"GREEKS SUMMARY AUDIT: {total_valid} Valid Greeks Calculated, {total_invalid} Illiquid/Invalid.")
    print("=" * 70)
    return True

# ==============================================================================
# STANDALONE ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TFEX EOD Options Black-76 Full Greeks Engine")
    parser.add_argument("--date", type=str, default=None, help="Target trade date (YYYY-MM-DD)")
    parser.add_argument("--local-db", type=str, default=None, help="Path to local SQLite db")
    args = parser.parse_args()

    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")
    pipe_url = f"{turso_url.replace('libsql://', 'https://').rstrip('/')}/v2/pipeline" if turso_url else None

    run_eod_greeks_pipeline(
        pipeline_url=pipe_url,
        auth_token=turso_token,
        db_file=args.local_db,
        target_date=args.date
    )
