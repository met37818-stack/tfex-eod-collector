"""
Unit tests for strict fail-loud data honesty and tiered retention in tfex-eod-collector
"""
import datetime
import os
import sqlite3
import tempfile
import unittest

from greeks_engine import select_dynamic_thor_rate, compute_greeks_records_for_date
from eod_collector import run_tiered_retention_guard


class TestEODGreeksEngine(unittest.TestCase):

    def test_thor_rate_fail_loud_empty(self):
        empty_thor = {"overnight": None, "1m": None, "3m": None, "6m": None}
        with self.assertRaises(ValueError):
            select_dynamic_thor_rate(empty_thor, dte=30)

    def test_greeks_strict_settlement_price_only(self):
        # When settlement_price is None or 0, even if close has a price,
        # it should NOT fallback to close. iv must be None and Greeks must be None.
        options_rows = [
            {
                "symbol": "S50Z26C900",
                "series": "Z26",
                "option_type": "C",
                "strike": 900.0,
                "settlement_price": None,  # Missing settlement price
                "close": 25.0,            # Close exists but MUST NOT be used as fallback!
                "oi": 100
            }
        ]
        futures_dict = {"Z26": 920.0, "S50Z26": 920.0}
        thor_dict = {"overnight": 0.02, "1m": 0.021, "3m": 0.022, "6m": 0.023}

        recs, val_cnt, inv_cnt = compute_greeks_records_for_date(
            trade_date="2026-11-19",
            options_rows=options_rows,
            futures_dict=futures_dict,
            thor_rate=thor_dict,
            now_bkk_str="2026-11-19 18:00:00 ICT"
        )

        self.assertEqual(len(recs), 1)
        # iv and delta must be None because settlement_price was None
        self.assertIsNone(recs[0]["iv"])
        self.assertIsNone(recs[0]["delta"])
        self.assertEqual(inv_cnt, 1)
        self.assertEqual(val_cnt, 0)

    def test_greeks_strict_thor_rate_raises(self):
        options_rows = [
            {
                "symbol": "S50Z26C900",
                "series": "Z26",
                "option_type": "C",
                "strike": 900.0,
                "settlement_price": 25.0,
                "close": 25.0,
                "oi": 100
            }
        ]
        futures_dict = {"Z26": 920.0, "S50Z26": 920.0}
        # Invalid thor rate (None or <= 0)
        with self.assertRaises(ValueError):
            compute_greeks_records_for_date(
                trade_date="2026-11-19",
                options_rows=options_rows,
                futures_dict=futures_dict,
                thor_rate=None,
                now_bkk_str="2026-11-19 18:00:00 ICT"
            )


class TestTieredRetentionPolicy(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db_path = self.tmp.name
        self.tmp.close()

        self.conn = sqlite3.connect(self.db_path)
        cur = self.conn.cursor()

        # Create Intraday Tables
        cur.execute("""
        CREATE TABLE options_snapshots (
            captured_at_bkk TEXT,
            symbol TEXT,
            strike REAL
        );
        """)
        cur.execute("""
        CREATE TABLE futures_snapshots (
            captured_at_bkk TEXT,
            symbol TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE spot_snapshots (
            captured_at_bkk TEXT,
            last REAL
        );
        """)

        # Create Permanent EOD & Analytics Tables
        cur.execute("""
        CREATE TABLE eod_options_series (
            trade_date TEXT,
            symbol TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE daily_realized_vol (
            trade_date TEXT,
            rv REAL
        );
        """)
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_tiered_retention_lifespans_and_permanence(self):
        base_date = datetime.date(2026, 11, 19)
        cur = self.conn.cursor()

        # Tier 1: options_snapshots (14-day limit)
        # Old (20 days ago) -> 2026-10-30 -> SHOULD BE PURGED
        cur.execute("INSERT INTO options_snapshots VALUES ('2026-10-30 10:00:00', 'S50Z26C900', 900.0);")
        # Recent (10 days ago) -> 2026-11-09 -> SHOULD BE KEPT
        cur.execute("INSERT INTO options_snapshots VALUES ('2026-11-09 10:00:00', 'S50Z26C920', 920.0);")

        # Tier 2: futures_snapshots & spot_snapshots (90-day limit)
        # Old (100 days ago) -> 2026-08-11 -> SHOULD BE PURGED
        cur.execute("INSERT INTO futures_snapshots VALUES ('2026-08-11 10:00:00', 'S50U26');")
        # Recent (80 days ago) -> 2026-08-31 -> SHOULD BE KEPT
        cur.execute("INSERT INTO futures_snapshots VALUES ('2026-08-31 10:00:00', 'S50Z26');")

        # Old spot (95 days ago) -> 2026-08-16 -> SHOULD BE PURGED
        cur.execute("INSERT INTO spot_snapshots VALUES ('2026-08-16 10:00:00', 915.0);")
        # Recent spot (30 days ago) -> 2026-10-20 -> SHOULD BE KEPT
        cur.execute("INSERT INTO spot_snapshots VALUES ('2026-10-20 10:00:00', 925.0);")

        # Tier 3: eod_* tables & daily_realized_vol (PERMANENT - 300 days ago -> NEVER DELETED)
        cur.execute("INSERT INTO eod_options_series VALUES ('2026-01-23', 'S50H26C900');")
        cur.execute("INSERT INTO daily_realized_vol VALUES ('2026-01-23', 0.142);")

        self.conn.commit()

        # Run Tiered Retention Guard
        res = run_tiered_retention_guard(db_file=self.db_path, ref_date=base_date)

        # Verify returned deleted counts
        self.assertEqual(res["options_snapshots"], 1)
        self.assertEqual(res["futures_snapshots"], 1)
        self.assertEqual(res["spot_snapshots"], 1)

        # Verify surviving records in DB
        cur.execute("SELECT captured_at_bkk, symbol FROM options_snapshots;")
        opt_survivors = cur.fetchall()
        self.assertEqual(len(opt_survivors), 1)
        self.assertEqual(opt_survivors[0][0], "2026-11-09 10:00:00")

        cur.execute("SELECT captured_at_bkk, symbol FROM futures_snapshots;")
        fut_survivors = cur.fetchall()
        self.assertEqual(len(fut_survivors), 1)
        self.assertEqual(fut_survivors[0][0], "2026-08-31 10:00:00")

        cur.execute("SELECT captured_at_bkk, last FROM spot_snapshots;")
        spot_survivors = cur.fetchall()
        self.assertEqual(len(spot_survivors), 1)
        self.assertEqual(spot_survivors[0][0], "2026-10-20 10:00:00")

        # Verify Tier 3 permanence
        cur.execute("SELECT COUNT(*) FROM eod_options_series;")
        self.assertEqual(cur.fetchone()[0], 1)

        cur.execute("SELECT COUNT(*) FROM daily_realized_vol;")
        self.assertEqual(cur.fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
