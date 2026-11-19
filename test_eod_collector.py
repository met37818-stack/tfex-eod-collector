"""
Unit tests for strict fail-loud data honesty in tfex-eod-collector
"""
import unittest
from greeks_engine import select_dynamic_thor_rate, compute_greeks_records_for_date


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


if __name__ == "__main__":
    unittest.main()
