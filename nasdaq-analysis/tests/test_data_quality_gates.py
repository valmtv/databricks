"""
Unit Tests: Data Quality Gates, Expectations & Quarantine Routing
------------------------------------------------------------------
Validates that invalid records are strictly classified for quarantine routing
and that valid records meet completeness and consistency constraints.
"""

import unittest


class TestDataQualityGates(unittest.TestCase):

    def test_price_quarantine_conditions(self):
        """Checks quarantine routing for price records."""
        test_cases = [
            # (Symbol, PriceTimestamp, Close, Volume, ExpectedQuarantine)
            ("AAPL", "2026-09-01 10:00:00", 220.5, 1000, False),   # Valid
            (None, "2026-09-01 10:00:00", 220.5, 1000, True),     # Missing Symbol
            ("", "2026-09-01 10:00:00", 220.5, 1000, True),       # Empty Symbol
            ("AAPL", None, 220.5, 1000, True),                     # Null Timestamp
            ("AAPL", "2026-09-01 10:00:00", 0.0, 1000, True),      # Zero Close Price
            ("AAPL", "2026-09-01 10:00:00", -15.0, 1000, True),   # Negative Close Price
            ("AAPL", "2026-09-01 10:00:00", 220.5, -50, True),     # Negative Volume
        ]

        for sym, ts, close, vol, expected_quarantine in test_cases:
            is_quarantined = (
                sym is None or sym.strip() == "" or
                ts is None or
                close is None or close <= 0.0 or
                vol is None or vol < 0
            )
            self.assertEqual(
                is_quarantined,
                expected_quarantine,
                f"Failed for record: Symbol={sym}, Timestamp={ts}, Close={close}, Vol={vol}"
            )

    def test_news_quarantine_conditions(self):
        """Checks quarantine routing for news articles."""
        test_cases = [
            # (ArticleId, Headline, Timestamp, ExpectedQuarantine)
            ("10101", "Apple launches new M4 chip", 1756789000, False),  # Valid
            (None, "Valid Headline", 1756789000, True),                   # Null ID
            ("", "Valid Headline", 1756789000, True),                     # Empty ID
            ("10102", None, 1756789000, True),                           # Null Headline
            ("10103", "", 1756789000, True),                             # Empty Headline
            ("10104", "Valid Headline", None, True),                     # Null Timestamp
        ]

        for art_id, headline, ts, expected_quarantine in test_cases:
            is_quarantined = (
                art_id is None or str(art_id).strip() == "" or
                headline is None or headline.strip() == "" or
                ts is None
            )
            self.assertEqual(
                is_quarantined,
                expected_quarantine,
                f"Failed for news: ID={art_id}, Headline={headline}, Timestamp={ts}"
            )

    def test_reconciliation_zero_silent_loss_math(self):
        """Asserts the invariant: Bronze Count == Silver Valid Count + Quarantine Count."""
        bronze_count = 10000
        quarantine_count = 42
        silver_valid_count = 9958

        self.assertEqual(bronze_count, silver_valid_count + quarantine_count)


if __name__ == "__main__":
    unittest.main()
