"""SET / TFEX trading calendar — single source of truth.

Before this module existed, `eod_collector.py` and `greeks_engine.py` each kept
their own `SET_HOLIDAYS` set and their own `get_penultimate_business_day()`.
The two lists disagreed (the greeks copy was missing every 2027 holiday), so the
same contract could be assigned two different expiry dates — and therefore two
different DTEs — depending on which module computed it. Checked case: S50Z27
expired 2027-12-29 per the collector but 2027-12-30 per the greeks engine.

Contracts expire on the penultimate business day of the contract month
(H = March, M = June, U = September, Z = December).

Holiday sources (update once per year, when SET publishes the next calendar):
- 2026: BOT notification "Financial institutions' holidays and additional
  special holiday for B.E. 2569 (2026)" dated 5 Aug 2025, plus the SET
  calendar amendments dated 16 Jun 2026 and 19 Nov 2026 — 20 weekday closures.
  https://www.set.or.th/en/about/event-calendar/holiday?year=2026
- 2027: https://www.set.or.th/en/about/event-calendar/holiday?year=2027
  — 18 weekday closures.

2026-10-16 (the IMF/WBG annual-meeting closure) is a full market closure.
Verified against the exchange feed: TFEX published no daily statistics for
2026-10-16 while 2026-10-14, 15, 19 and 20 were normal sessions. October is not
a contract month, so it cannot shift a contract expiry, but the collector must
not treat that day as a trading session.

Only weekday closures belong in this set — `is_trading_day()` already excludes
Saturday and Sunday.
"""

import calendar
import datetime

SET_HOLIDAYS = frozenset({
    # 2026 (20 weekday closures)
    "2026-01-01",  # New Year's Day
    "2026-01-02",  # Additional special holiday
    "2026-03-03",  # Makha Bucha Day
    "2026-04-06",  # Chakri Memorial Day
    "2026-04-13",  # Songkran Festival
    "2026-04-14",  # Songkran Festival
    "2026-04-15",  # Songkran Festival
    "2026-05-01",  # National Labour Day
    "2026-05-04",  # Coronation Day
    "2026-06-01",  # Substitution for Visakha Bucha Day (Sun 31 May 2026)
    "2026-06-03",  # H.M. Queen Suthida's Birthday
    "2026-07-28",  # H.M. King Maha Vajiralongkorn's Birthday
    "2026-07-29",  # Asarnha Bucha Day
    "2026-08-12",  # H.M. Queen Sirikit The Queen Mother's Birthday
    "2026-10-13",  # King Bhumibol Adulyadej Memorial Day
    "2026-10-16",  # Additional special holiday (IMF/WBG Annual Meetings)
    "2026-10-23",  # Chulalongkorn Day
    "2026-12-07",  # Substitution for Father's Day (Sat 5 Dec 2026)
    "2026-12-10",  # Constitution Day
    "2026-12-31",  # New Year's Eve
    # 2027 (18 weekday closures)
    "2027-01-01",  # New Year's Day
    "2027-02-22",  # Substitution for Makha Bucha Day (Sun 21 Feb 2027)
    "2027-04-06",  # Chakri Memorial Day
    "2027-04-13",  # Songkran Festival
    "2027-04-14",  # Songkran Festival
    "2027-04-15",  # Songkran Festival
    "2027-05-03",  # Substitution for National Labour Day (Sat 1 May 2027)
    "2027-05-04",  # Coronation Day
    "2027-05-20",  # Visakha Bucha Day
    "2027-06-03",  # H.M. Queen Suthida's Birthday
    "2027-07-19",  # Substitution for Asarnha Bucha Day (Sun 18 Jul 2027)
    "2027-07-28",  # H.M. King Maha Vajiralongkorn's Birthday
    "2027-08-12",  # H.M. Queen Sirikit The Queen Mother's Birthday
    "2027-10-13",  # King Bhumibol Adulyadej Memorial Day
    "2027-10-25",  # Substitution for Chulalongkorn Day (Sat 23 Oct 2027)
    "2027-12-06",  # Substitution for Father's Day (Sun 5 Dec 2027)
    "2027-12-10",  # Constitution Day
    "2027-12-31",  # New Year's Eve
})

QUARTER_MONTHS = (3, 6, 9, 12)
MONTH_CODES = {3: "H", 6: "M", 9: "U", 12: "Z"}
QUARTER_MONTH_BY_CODE = {code: month for month, code in MONTH_CODES.items()}


def _key(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def is_trading_day(d: datetime.date, holidays: frozenset = SET_HOLIDAYS) -> bool:
    """True when `d` is a weekday and not an official SET holiday."""
    return d.weekday() < 5 and _key(d) not in holidays


def get_penultimate_business_day(
    year: int,
    month: int,
    holidays: frozenset = SET_HOLIDAYS,
) -> datetime.date:
    """Penultimate business day of `month` — the TFEX expiry date for that month."""
    _, last_day = calendar.monthrange(year, month)
    biz_days = [
        datetime.date(year, month, d)
        for d in range(1, last_day + 1)
        if is_trading_day(datetime.date(year, month, d), holidays)
    ]
    if len(biz_days) < 2:
        return biz_days[-1] if biz_days else datetime.date(year, month, last_day)
    return biz_days[-2]


def get_series_expiry_date(series_code: str) -> datetime.date:
    """Expiry date for a quarterly series, e.g. 'S50Z26' or 'Z26' -> 2026-12-29."""
    code = series_code.upper()
    if code.startswith("S50"):
        quarter_char, year_suffix = code[3], code[4:6]
    else:
        quarter_char, year_suffix = code[0], code[1:3]

    quarter_month = QUARTER_MONTH_BY_CODE.get(quarter_char, 12)
    return get_penultimate_business_day(2000 + int(year_suffix), quarter_month)
