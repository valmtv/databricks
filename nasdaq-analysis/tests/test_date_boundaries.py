"""
Unit Tests: Date Range Boundary Math & Market Window Invariants
-----------------------------------------------------------------
Validates:
1. yfinance inclusive-to-exclusive date expansion (+1 day).
2. Weekend detection and trading closure invariants.
3. Elapsed time delta evaluation (holding off-hours news in PENDING).
"""

import unittest
from datetime import datetime, timedelta, date


def resolve_yfinance_end_date(start_date: str, end_date: str = None) -> tuple:
    """Calculates inclusive user dates and yfinance exclusive query date."""
    start = start_date.strip()
    inclusive_end = (end_date or start).strip()
    # yfinance exclusive end parameter: inclusive_end + 1 day
    dt_end = datetime.strptime(inclusive_end, "%Y-%m-%d")
    yf_exclusive_end = (dt_end + timedelta(days=1)).strftime("%Y-%m-%d")
    return start, inclusive_end, yf_exclusive_end


def is_market_closed_on_date(dt_str: str) -> bool:
    """Checks if a date falls on a Saturday or Sunday."""
    d = datetime.strptime(dt_str, "%Y-%m-%d").date()
    return d.weekday() in (5, 6)


def evaluate_event_window_eligibility(news_ts_str: str, first_price_ts_str: str) -> str:
    """
    Evaluates whether an article is eligible for completion or must remain pending.
    Condition: Both timestamps exist and the gap between news and first price is <= 24 hours.
    """
    if not news_ts_str or not first_price_ts_str:
        return "PENDING_EVALUATION"
    t_news = datetime.strptime(news_ts_str, "%Y-%m-%d %H:%M:%S")
    t_price = datetime.strptime(first_price_ts_str, "%Y-%m-%d %H:%M:%S")
    delta_hours = (t_price - t_news).total_seconds() / 3600.0
    if delta_hours < 0 or delta_hours > 24.0:
        return "PENDING_EVALUATION"
    return "COMPLETED"


class TestDateBoundaries(unittest.TestCase):

    def test_single_day_yfinance_conversion(self):
        """Single-day input (2026-08-28) must produce exclusive end date 2026-08-29."""
        start, inc_end, yf_end = resolve_yfinance_end_date("2026-08-28", "2026-08-28")
        self.assertEqual(start, "2026-08-28")
        self.assertEqual(inc_end, "2026-08-28")
        self.assertEqual(yf_end, "2026-08-29")

    def test_weekly_range_yfinance_conversion(self):
        """Mon-Fri input (2026-08-24 to 2026-08-28) must produce exclusive end 2026-08-29."""
        start, inc_end, yf_end = resolve_yfinance_end_date("2026-08-24", "2026-08-28")
        self.assertEqual(start, "2026-08-24")
        self.assertEqual(inc_end, "2026-08-28")
        self.assertEqual(yf_end, "2026-08-29")

    def test_weekend_closure_detection(self):
        """Saturday (2026-08-29) and Sunday (2026-08-30) must be flagged as market closed."""
        self.assertFalse(is_market_closed_on_date("2026-08-28")) # Friday = Open
        self.assertTrue(is_market_closed_on_date("2026-08-29"))  # Saturday = Closed
        self.assertTrue(is_market_closed_on_date("2026-08-30"))  # Sunday = Closed
        self.assertFalse(is_market_closed_on_date("2026-08-31")) # Monday = Open

    def test_weekend_news_held_in_pending(self):
        """News published Saturday (2026-08-29 14:00) with first trade Monday (2026-08-31 09:30) > 43h."""
        status = evaluate_event_window_eligibility(
            news_ts_str="2026-08-29 14:00:00",
            first_price_ts_str="2026-08-31 09:30:00"
        )
        self.assertEqual(status, "PENDING_EVALUATION")


if __name__ == "__main__":
    unittest.main()
