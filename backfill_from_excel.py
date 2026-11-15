#!/usr/bin/env python3
"""
One-off Backfill Script: Imports historical TFEX & BOT data from Excel (historical_eod.xlsx)
into Turso Cloud SQLite.

Uses `INSERT OR IGNORE INTO` via Turso HTTP Pipeline API (/v2/pipeline)
to safely fill missing historical dates without overwriting fresh data.
"""

import os
import sys
import re
import time
import datetime
from pathlib import Path

import requests
import openpyxl

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

def parse_date(val) -> str | None:
    """Safely parse various Excel date formats into YYYY-MM-DD."""
    if val is None:
        return None
    if isinstance(val, (datetime.datetime, datetime.date)):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    if not s or s in ("-", "None", "null", "N/A"):
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d %b %Y", "%Y/%m/%d"):
        try:
            return datetime.datetime.strptime(s[:11].strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def parse_float(val) -> float | None:
    """Safe float parser handling commas, dashes, and strings."""
    if val is None:
        return None
    s = str(val).replace(",", "").strip()
    if s in ("", "-", "None", "null", "N/A"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None

def parse_int(val) -> int | None:
    """Safe integer parser handling commas, plus signs, dashes, and strings."""
    if val is None:
        return None
    s = str(val).replace(",", "").replace("+", "").strip()
    if s in ("", "-", "None", "null", "N/A"):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None

def find_excel_file() -> str:
    """Locate the Excel file (historical_eod.xlsx or historical_eod.xlsx.xlsx)."""
    candidates = [
        "historical_eod.xlsx",
        "historical_eod.xlsx.xlsx",
        os.path.join(os.path.dirname(__file__), "historical_eod.xlsx"),
        os.path.join(os.path.dirname(__file__), "historical_eod.xlsx.xlsx"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    raise FileNotFoundError("Could not find historical_eod.xlsx in current directory.")

# ==============================================================================
# TURSO PIPELINE EXECUTION
# ==============================================================================

def get_turso_table_counts(pipeline_url: str, auth_token: str) -> dict[str, int]:
    """Query current row counts for all 5 tables from Turso."""
    tables = [
        "eod_investor_types",
        "eod_market_stats",
        "eod_futures_series",
        "eod_options_series",
        "eod_thor_rates"
    ]
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }
    requests_payload = [{"type": "execute", "stmt": {"sql": f"SELECT COUNT(*) FROM {t}"}} for t in tables]
    requests_payload.append({"type": "close"})

    resp = requests.post(pipeline_url, headers=headers, json={"requests": requests_payload}, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to query table counts ({resp.status_code}): {resp.text}")

    counts = {}
    res_json = resp.json()
    for t, item in zip(tables, res_json.get("results", [])):
        try:
            val = item["response"]["result"]["rows"][0][0]["value"]
            counts[t] = int(val)
        except Exception:
            counts[t] = 0
    return counts

def execute_turso_pipeline(pipeline_url: str, auth_token: str, statements: list[dict], batch_size: int = 150):
    """
    Execute batch statements against Turso Cloud SQLite via HTTP Pipeline API.
    """
    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    total = len(statements)
    print(f"[Turso Pipeline] Sending {total} statements in batches of {batch_size}...")

    start_t = time.time()
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
            timeout=40
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Turso Pipeline failed ({resp.status_code}): {resp.text}")

        res_json = resp.json()
        for idx, result in enumerate(res_json.get("results", [])):
            if result.get("type") == "error":
                raise RuntimeError(f"Turso statement execution error at index {idx}: {result.get('error')}")

        batch_idx = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size
        print(f"  -> Batch {batch_idx}/{total_batches} ({len(chunk)} stmts) completed.")

    elapsed = time.time() - start_t
    print(f"[Turso Pipeline] Successfully committed all {total} statements in {elapsed:.2f} seconds.")

# ==============================================================================
# EXCEL PARSING & STATEMENT GENERATION
# ==============================================================================

def parse_excel_to_statements(excel_path: str) -> tuple[list[dict], dict[str, int]]:
    """
    Reads all sheets from Excel and generates INSERT OR IGNORE statements.
    """
    print(f"[Excel] Loading workbook from {excel_path} (data_only=True)...")
    wb = openpyxl.load_workbook(excel_path, data_only=True)
    sheet_names = wb.sheetnames
    print(f"[Excel] Discovered {len(sheet_names)} total sheets.")

    statements = []
    category_counts = {
        "eod_investor_types": 0,
        "eod_market_stats": 0,
        "eod_futures_series": 0,
        "eod_options_series": 0,
        "eod_thor_rates": 0,
    }

    # 1. Investor Sheets
    inv_map = {
        "Investor_Futures": "Equity Index Futures",
        "Investor_Call": "Equity Index Call Options",
        "Investor_Put": "Equity Index Put Options",
    }
    sql_inv = """
    INSERT OR IGNORE INTO eod_investor_types (
        trade_date, category, inst_long, inst_short, inst_net,
        foreign_long, foreign_short, foreign_net,
        local_long, local_short, local_net, total_volume
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for s_name, cat in inv_map.items():
        if s_name not in wb.sheetnames:
            continue
        ws = wb[s_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 2:
            continue
        count = 0
        for r in rows[1:]:
            t_date = parse_date(r[0])
            if not t_date:
                continue
            i_long = parse_int(r[1])
            i_short = parse_int(r[2])
            i_net = parse_int(r[3])
            f_long = parse_int(r[4])
            f_short = parse_int(r[5])
            f_net = parse_int(r[6])
            l_long = parse_int(r[7])
            l_short = parse_int(r[8])
            l_net = parse_int(r[9])
            tot_vol = (i_long or 0) + (f_long or 0) + (l_long or 0)
            statements.append({
                "sql": sql_inv,
                "args": [t_date, cat, i_long, i_short, i_net, f_long, f_short, f_net, l_long, l_short, l_net, tot_vol]
            })
            count += 1
        category_counts["eod_investor_types"] += count

    # 2. Market Stats Sheets
    mkt_map = {
        "Market_Stats_Futures": ("TXI_F", "SET50_FC"),
        "Market_Stats_Call": ("TXI_C", "SET50_OEC"),
        "Market_Stats_Put": ("TXI_P", "SET50_OEP"),
    }
    sql_mkt = """
    INSERT OR IGNORE INTO eod_market_stats (
        trade_date, market_id, instrument_id, open, high, low,
        settlement_price, chg, pct_chg, volume, oi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for s_name, (m_id, i_id) in mkt_map.items():
        if s_name not in wb.sheetnames:
            continue
        ws = wb[s_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 2:
            continue
        count = 0
        for r in rows[1:]:
            t_date = parse_date(r[0])
            if not t_date:
                continue
            statements.append({
                "sql": sql_mkt,
                "args": [
                    t_date, m_id, i_id,
                    parse_float(r[1]), parse_float(r[2]), parse_float(r[3]),
                    parse_float(r[4]), parse_float(r[5]), parse_float(r[6]),
                    parse_int(r[7]), parse_int(r[8])
                ]
            })
            count += 1
        category_counts["eod_market_stats"] += count

    # 3. Futures Series Sheets (e.g. S50Z26, S50H27)
    fut_regex = re.compile(r"^S50[HMUZ]\d{2}$")
    sql_fut = """
    INSERT OR IGNORE INTO eod_futures_series (
        trade_date, series, open, high, low, close,
        settlement_price, chg, pct_chg, volume, oi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for s_name in sheet_names:
        if not fut_regex.match(s_name):
            continue
        ws = wb[s_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 2:
            continue
        count = 0
        for r in rows[1:]:
            t_date = parse_date(r[0])
            if not t_date:
                continue
            statements.append({
                "sql": sql_fut,
                "args": [
                    t_date, s_name,
                    parse_float(r[1]), parse_float(r[2]), parse_float(r[3]), parse_float(r[4]),
                    parse_float(r[5]), parse_float(r[6]), parse_float(r[7]),
                    parse_int(r[8]), parse_int(r[9])
                ]
            })
            count += 1
        category_counts["eod_futures_series"] += count

    # 4. Options Series Sheets (e.g. S50Z26C1000)
    opt_regex = re.compile(r"^(S50[HMUZ]\d{2})([CP])(\d+)$")
    sql_opt = """
    INSERT OR IGNORE INTO eod_options_series (
        trade_date, symbol, series, option_type, strike, open, high, low, close,
        settlement_price, chg, pct_chg, volume, oi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    for s_name in sheet_names:
        match = opt_regex.match(s_name)
        if not match:
            continue
        ser, opt_type, strike_str = match.groups()
        strike_val = float(strike_str)

        ws = wb[s_name]
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 2:
            continue
        count = 0
        for r in rows[1:]:
            t_date = parse_date(r[0])
            if not t_date:
                continue
            statements.append({
                "sql": sql_opt,
                "args": [
                    t_date, s_name, ser, opt_type, strike_val,
                    parse_float(r[1]), parse_float(r[2]), parse_float(r[3]), parse_float(r[4]),
                    parse_float(r[5]), parse_float(r[6]), parse_float(r[7]),
                    parse_int(r[8]), parse_int(r[9])
                ]
            })
            count += 1
        category_counts["eod_options_series"] += count

    # 5. THOR Sheet
    if "THOR" in sheet_names:
        ws = wb["THOR"]
        rows = list(ws.iter_rows(values_only=True))
        sql_thor = """
        INSERT OR IGNORE INTO eod_thor_rates (
            trade_date, thor_rate, thor_1m_avg, thor_3m_avg, thor_6m_avg
        ) VALUES (?, ?, ?, ?, ?);
        """
        count = 0
        if len(rows) >= 2:
            for r in rows[1:]:
                t_date = parse_date(r[0])
                if not t_date:
                    continue
                statements.append({
                    "sql": sql_thor,
                    "args": [
                        t_date,
                        parse_float(r[1]), parse_float(r[2]), parse_float(r[3]), parse_float(r[4])
                    ]
                })
                count += 1
        category_counts["eod_thor_rates"] += count

    return statements, category_counts

# ==============================================================================
# MAIN BACKFILL RUNNER
# ==============================================================================

def main():
    print("=" * 75)
    print("TFEX & BOT HISTORICAL BACKFILL TO TURSO CLOUD SQLITE")
    print(f"Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 75)

    # 1. Load Environment & Turso Credentials
    load_dotenv()
    turso_url = os.environ.get("TURSO_DATABASE_URL")
    turso_token = os.environ.get("TURSO_AUTH_TOKEN")

    if not turso_url or not turso_token:
        print("[ERROR] TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not set in environment or .env")
        sys.exit(1)

    pipeline_url = normalize_turso_url(turso_url)
    print(f"[Turso] Database Target: {pipeline_url}")

    # 2. Query Row Counts BEFORE Backfill
    print("[Turso] Fetching initial table row counts...")
    before_counts = get_turso_table_counts(pipeline_url, turso_token)
    print("  Baseline Counts:")
    for t, cnt in before_counts.items():
        print(f"    - {t:<22} : {cnt:>6} rows")

    # 3. Locate & Parse Excel File
    excel_path = find_excel_file()
    print(f"[Excel] Using file: {excel_path}")

    statements, extracted_counts = parse_excel_to_statements(excel_path)
    print(f"[Excel] Total statements prepared: {len(statements)}")
    for cat, cnt in extracted_counts.items():
        print(f"    - {cat:<22} : {cnt:>6} candidates")

    # 4. Execute Backfill via Turso Pipeline
    execute_turso_pipeline(pipeline_url, turso_token, statements, batch_size=150)

    # 5. Query Row Counts AFTER Backfill
    print("[Turso] Fetching updated table row counts...")
    after_counts = get_turso_table_counts(pipeline_url, turso_token)

    # 6. Detailed Audit Summary
    print("=" * 75)
    print("HISTORICAL BACKFILL AUDIT SUMMARY (BEFORE vs AFTER)")
    print("=" * 75)
    print(f"{'Table Name':<25} | {'Before':>10} | {'Candidates':>10} | {'After':>10} | {'New Added':>10}")
    print("-" * 75)
    total_before = 0
    total_after = 0
    total_added = 0

    for t in before_counts:
        b = before_counts[t]
        c = extracted_counts.get(t, 0)
        a = after_counts.get(t, 0)
        diff = a - b
        total_before += b
        total_after += a
        total_added += diff
        print(f"{t:<25} | {b:>10} | {c:>10} | {a:>10} | {diff:>10}")

    print("-" * 75)
    print(f"{'TOTAL':<25} | {total_before:>10} | {len(statements):>10} | {total_after:>10} | {total_added:>10}")
    print("=" * 75)
    print("SUCCESS: Backfill completed safely with INSERT OR IGNORE. Fresh data preserved.")

if __name__ == "__main__":
    main()
