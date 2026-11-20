"""
Unit tests for strict fail-loud data honesty and tiered retention in tfex-eod-collector
"""
import contextlib
import datetime
import io
import os
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

import requests

from greeks_engine import (
    select_dynamic_thor_rate,
    compute_greeks_records_for_date,
    calculate_implied_vol,
    calculate_black76_full_greeks,
    black76_price,
)
from calendar_utils import SET_HOLIDAYS, get_penultimate_business_day, get_series_expiry_date
from eod_collector import (
    get_active_quarterly_series,
    resolve_target_trade_date,
    run_tiered_retention_guard,
)


class TestEODGreeksEngine(unittest.TestCase):

    def test_thor_rate_fail_loud_empty(self):
        empty_thor = {"overnight": None, "1m": None, "3m": None, "6m": None}
        with self.assertRaises(ValueError):
            select_dynamic_thor_rate(empty_thor, dte=30)

    def test_thor_percent_is_always_converted(self):
        # BOT publishes THOR in percent. The old magnitude heuristic (>0.5 means
        # percent, otherwise decimal) read a real 0.45% rate as r=0.45 (45% p.a.).
        self.assertAlmostEqual(select_dynamic_thor_rate({"overnight": 1.25}, 10)[0], 0.0125)
        self.assertAlmostEqual(select_dynamic_thor_rate({"overnight": 0.45}, 10)[0], 0.0045)
        self.assertAlmostEqual(select_dynamic_thor_rate({"overnight": 0.50}, 10)[0], 0.0050)
        self.assertAlmostEqual(select_dynamic_thor_rate({"overnight": 0.05}, 10)[0], 0.0005)
        with self.assertRaises(ValueError):
            select_dynamic_thor_rate({"overnight": 45.0}, 10)

    def test_iv_recovers_true_sigma_with_low_rate(self):
        # Regression for the x100 rate bug: a 60-day OTM option priced at a true
        # 25% vol and a 0.45% rate must solve back to 25%, not ~26.3%.
        F, K, T, sigma = 920.0, 950.0, 60 / 365.0, 0.25
        price = black76_price(F, K, T, 0.0045, sigma, "C")
        iv = calculate_implied_vol(price, F, K, T, 0.0045, "C")
        self.assertAlmostEqual(iv, sigma, delta=1e-3)

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
        # THOR values are percent (BOT publishes 1.25 == 1.25% p.a.). This fixture
        # used decimal values before the unit bug was fixed.
        thor_dict = {"overnight": 2.0, "1m": 2.1, "3m": 2.2, "6m": 2.3}

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

    def test_iv_capped_at_150_pct(self):
        # Deep OTM or unrealistic price should not return IV > 1.50
        F = 920.0
        K = 1100.0  # Far OTM Call
        T = 30.0 / 365.0
        r = 0.02
        # A high price for far OTM call requires absurd IV > 150%
        unrealistic_price = 100.0
        iv = calculate_implied_vol(unrealistic_price, F, K, T, r, "C")
        self.assertIsNone(iv, "IV exceeding 1.50 (150%) must return None")

    def test_charm_dampened_at_low_dte(self):
        # When DTE <= 2, charm should be capped within [-5.0, 5.0]
        F = 920.0
        K = 920.0
        T = 1.0 / 365.0  # DTE = 1
        r = 0.02
        sigma = 0.15
        greeks = calculate_black76_full_greeks(F, K, T, r, sigma, "C")
        self.assertLessEqual(abs(greeks["charm"]), 5.0)

    def test_active_quarterly_series_penultimate_business_day(self):
        # In Dec 2026, penultimate business day is 2026-12-29
        # Check active quarterly discovery
        p_day = get_penultimate_business_day(2026, 12)
        self.assertEqual(p_day.strftime("%Y-%m-%d"), "2026-12-29")
        active = get_active_quarterly_series(datetime.datetime(2026, 11, 20, 10, 0, 0))
        self.assertEqual(active[0], "S50Z26")

    def test_expiry_matches_set_calendar(self):
        # Single shared calendar. The greeks engine used to carry its own 19-entry
        # copy with no 2027 holidays at all, so it dated S50Z27 to 2027-12-30 and
        # gave every one of its options a DTE one day too long.
        self.assertEqual(get_penultimate_business_day(2026, 12), datetime.date(2026, 12, 29))
        self.assertEqual(get_penultimate_business_day(2027, 12), datetime.date(2027, 12, 29))
        self.assertEqual(get_series_expiry_date("S50Z26"), datetime.date(2026, 12, 29))
        self.assertEqual(get_series_expiry_date("S50Z27"), datetime.date(2027, 12, 29))
        self.assertEqual(get_series_expiry_date("H27"), datetime.date(2027, 3, 30))

    def test_holiday_list_is_complete(self):
        # Official weekday closures: 19 in 2026, 18 in 2027 (set.or.th calendar).
        self.assertEqual(len([d for d in SET_HOLIDAYS if d.startswith("2026-")]), 19)
        self.assertEqual(len([d for d in SET_HOLIDAYS if d.startswith("2027-")]), 18)


class TestWorkerSessions(unittest.TestCase):
    """requests.Session must not be shared across the 8 scrape worker threads."""

    def test_worker_session_is_per_thread(self):
        from eod_collector import _worker_session

        main_session = _worker_session()
        self.assertIs(main_session, _worker_session(), "same thread must reuse its session")

        seen = {}

        def grab():
            seen["worker"] = _worker_session()

        thread = threading.Thread(target=grab)
        thread.start()
        thread.join()

        self.assertIsNot(main_session, seen["worker"], "worker thread must get its own session")

    def test_worker_session_inherits_warm_cookies(self):
        # TFEX (Imperva/Incapsula) answers 403 to a series request without the
        # session cookies handed out on the landing page, so a bare per-thread
        # session silently loses every option contract.
        import eod_collector as ec

        warm = requests.cookies.RequestsCookieJar()
        warm.set("incap_ses_357_3037351", "abc123", domain=".tfex.co.th", path="/")

        seen = {}

        def grab():
            seen["session"] = ec._worker_session(warm)

        thread = threading.Thread(target=grab)
        thread.start()
        thread.join()

        self.assertIn("incap_ses_357_3037351", seen["session"].cookies)
        self.assertEqual(seen["session"].cookies.get("incap_ses_357_3037351"), "abc123")


class TestTargetTradeDate(unittest.TestCase):
    """The trade_date a run must collect, before any network call."""

    def test_evening_run_targets_same_day(self):
        target, non_trading, _ = resolve_target_trade_date(datetime.datetime(2026, 11, 20, 21, 0))
        self.assertEqual(target, datetime.date(2026, 11, 20))
        self.assertFalse(non_trading)

    def test_early_morning_run_targets_previous_session(self):
        for hour, minute in ((1, 0), (6, 15)):
            target, non_trading, reason = resolve_target_trade_date(
                datetime.datetime(2026, 11, 23, hour, minute)
            )
            self.assertEqual(target, datetime.date(2026, 11, 20), reason)  # Friday
            self.assertFalse(non_trading)

    def test_weekend_target_is_non_trading(self):
        target, non_trading, reason = resolve_target_trade_date(datetime.datetime(2026, 11, 21, 9, 0))
        self.assertEqual(target, datetime.date(2026, 11, 21))
        self.assertTrue(non_trading)
        self.assertIn("weekend", reason)

    def test_holiday_target_is_non_trading(self):
        target, non_trading, reason = resolve_target_trade_date(datetime.datetime(2026, 12, 31, 10, 0))
        self.assertEqual(target, datetime.date(2026, 12, 31))
        self.assertTrue(non_trading)
        self.assertIn("holiday", reason)


class TestRetryHelper(unittest.TestCase):
    class _FakeResp:
        def __init__(self, status_code, headers=None, payload=None):
            self.status_code = status_code
            self.headers = headers or {}
            self.text = ""
            self._payload = payload if payload is not None else {}

        def json(self):
            return self._payload

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")

    def test_retries_429_then_succeeds(self):
        import eod_collector as ec

        responses = [self._FakeResp(429, {"Retry-After": "0"}), self._FakeResp(200, payload={"ok": True})]

        class FakeSession:
            def get(self, url, **kwargs):
                return responses.pop(0)

        with mock.patch.object(ec.time, "sleep", lambda *_: None):
            resp = ec.get_with_retry(FakeSession(), "https://example.invalid/x", attempts=3)
        self.assertEqual(resp.status_code, 200)

    def test_raises_after_attempts_exhausted(self):
        import eod_collector as ec

        class FakeSession:
            def get(self, url, **kwargs):
                raise requests.RequestException("boom")

        with mock.patch.object(ec.time, "sleep", lambda *_: None):
            with self.assertRaises(RuntimeError):
                ec.get_with_retry(FakeSession(), "https://example.invalid/x", attempts=2)


class TestOptionScrapeAccounting(unittest.TestCase):
    """A throttled fetch and a contract with no rows are different failures."""

    def test_errors_and_empty_contracts_are_counted_separately(self):
        import eod_collector as ec

        def payload_resp(payload):
            class R:
                status_code = 200
                headers = {}
                text = ""

                def json(self_inner):
                    return payload

            return R()

        class Worker:
            def get(self, url, **kwargs):
                if "S50Z26C900" in url:
                    raise requests.RequestException("throttled")
                if "S50Z26C920" in url:
                    return payload_resp({"data": []})
                return payload_resp({"data": [{
                    "date": "2026-11-20T00:00:00",
                    "settlementPrice": 12.5,
                    "totalVolume": 3,
                    "oi": 4,
                }]})

        class MainSession:
            cookies = requests.cookies.RequestsCookieJar()

            def get(self, url, **kwargs):
                class R:
                    status_code = 200
                    headers = {}
                    text = "<div>S50Z26C900 S50Z26C920 S50Z26C940</div>"

                    def raise_for_status(self_inner):
                        return None

                return R()

        with mock.patch.object(ec, "_worker_session", return_value=Worker()), \
                mock.patch.object(ec, "get_with_retry", side_effect=lambda s, u, **k: s.get(u, **k)):
            records, errors, no_data = ec.fetch_options_series(MainSession(), ["S50Z26"])

        self.assertEqual(errors, 1, "network error must be counted as a fetch error")
        self.assertEqual(no_data, 1, "HTTP 200 with no rows must be counted as no-data")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["symbol"], "S50Z26C940")


class TestTelegramTokenHygiene(unittest.TestCase):
    """The bot token is embedded in the request URL; it must never reach a log."""

    def test_token_never_reaches_stdout_on_transport_error(self):
        import eod_collector as ec

        token = "123456789:AAFakeTestTokenValueMustNotLeak"
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            cur = conn.cursor()
            cur.execute("CREATE TABLE eod_futures_series (trade_date TEXT, series TEXT)")
            cur.execute("CREATE TABLE eod_options_series (trade_date TEXT, symbol TEXT)")
            cur.execute("CREATE TABLE eod_investor_types (trade_date TEXT, category TEXT)")
            cur.execute("CREATE TABLE eod_market_stats (trade_date TEXT, market_id TEXT)")
            cur.execute("CREATE TABLE eod_thor_rates (trade_date TEXT, thor_rate REAL, thor_1m_avg REAL, thor_3m_avg REAL, thor_6m_avg REAL)")
            cur.execute("CREATE TABLE eod_telegram_log (trade_date TEXT PRIMARY KEY, notified_at_bkk TEXT, message_snippet TEXT)")
            cur.execute("INSERT INTO eod_futures_series VALUES ('2026-11-20', 'S50Z26')")
            cur.execute("INSERT INTO eod_thor_rates VALUES ('2026-11-20', 1.25, 1.20, 1.30, 1.40)")
            conn.commit()
            conn.close()

            transport_error = requests.ConnectionError(
                f"HTTPSConnectionPool(host='api.telegram.org'): Max retries exceeded with url: /bot{token}/sendMessage"
            )
            buffer = io.StringIO()
            with mock.patch.object(ec.requests, "post", side_effect=transport_error),                     contextlib.redirect_stdout(buffer):
                sent = ec.send_eod_telegram_alert(
                    bot_token=token,
                    chat_id="1",
                    db_file=tmp.name,
                    force_notify=True,
                )

            output = buffer.getvalue()
            self.assertFalse(sent)
            self.assertNotIn(token, output, "bot token leaked into stdout")
            self.assertIn("ConnectionError", output)
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)


class TestTursoArgEncoding(unittest.TestCase):
    """NaN/Inf are not valid JSON; they must degrade to NULL, not break a batch."""

    def test_nan_and_inf_become_null(self):
        import eod_collector as ec

        self.assertEqual(ec.to_turso_arg(float("nan")), {"type": "null"})
        self.assertEqual(ec.to_turso_arg(float("inf")), {"type": "null"})
        self.assertEqual(ec.to_turso_arg(1.5), {"type": "float", "value": 1.5})
        self.assertEqual(ec.to_turso_arg(True), {"type": "integer", "value": "1"})


class TestRetentionReporting(unittest.TestCase):
    """A DELETE whose row count cannot be confirmed must not be reported as 0."""

    def test_pipeline_affected_rows(self):
        import eod_collector as ec

        self.assertEqual(
            ec._pipeline_affected_rows({"type": "ok", "response": {"result": {"affected_row_count": 7}}}),
            7,
        )
        self.assertIsNone(ec._pipeline_affected_rows({"type": "error", "error": {"message": "no such table"}}))
        self.assertIsNone(ec._pipeline_affected_rows({"type": "ok", "response": {"result": {}}}))

    def test_format_deleted_marks_failure_not_zero(self):
        import eod_collector as ec

        self.assertIn("ล้มเหลว", ec._format_deleted(None))
        self.assertEqual(ec._format_deleted(0), "0 แถวถูกลบ")
        self.assertEqual(ec._format_deleted(12), "12 แถวถูกลบ")

    def test_missing_tables_report_zero_not_failure(self):
        import eod_collector as ec

        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            conn.execute("CREATE TABLE spot_snapshots (captured_at_bkk TEXT, last REAL);")
            conn.execute("INSERT INTO spot_snapshots VALUES ('2020-01-01 00:00:00', 900.0);")
            conn.commit()
            conn.close()

            result = ec.run_tiered_retention_guard(db_file=tmp.name, ref_date=datetime.date(2026, 11, 19))
            self.assertEqual(result["options_snapshots"], 0, "absent table is 0, not a failure")
            self.assertEqual(result["spot_snapshots"], 1, "count comes from the DELETE rowcount")
        finally:
            if os.path.exists(tmp.name):
                os.unlink(tmp.name)


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
