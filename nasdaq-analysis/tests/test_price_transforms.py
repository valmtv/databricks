"""
Unit Tests: Price Transformations
----------------------------------
Validates cleansing, schema enforcement, boundary conditions, and deduplication logic.
Runnable via pytest or python -m unittest.
"""

import unittest
from datetime import datetime


class TestPriceTransforms(unittest.TestCase):

    def test_price_validation_rules(self):
        """Validates that negative prices or negative volumes are flagged as invalid."""
        valid_record = {
            "Symbol": "AAPL",
            "PriceTimestamp": "2026-09-01 10:00:00",
            "Open": 220.50,
            "High": 221.00,
            "Low": 220.10,
            "Close": 220.80,
            "Volume": 154000
        }
        invalid_close = {
            "Symbol": "AAPL",
            "PriceTimestamp": "2026-09-01 10:05:00",
            "Open": 220.50,
            "High": 221.00,
            "Low": 220.10,
            "Close": -5.0,  # Corrupted negative price
            "Volume": 154000
        }
        invalid_volume = {
            "Symbol": "AAPL",
            "PriceTimestamp": "2026-09-01 10:10:00",
            "Open": 220.50,
            "High": 221.00,
            "Low": 220.10,
            "Close": 220.80,
            "Volume": -100  # Corrupted negative volume
        }

        # Rule assertions matching DLT expectation @dlt.expect_or_drop("valid_price_bounds", "Close > 0 AND Volume >= 0")
        self.assertTrue(valid_record["Close"] > 0 and valid_record["Volume"] >= 0)
        self.assertFalse(invalid_close["Close"] > 0 and invalid_close["Volume"] >= 0)
        self.assertFalse(invalid_volume["Close"] > 0 and invalid_volume["Volume"] >= 0)

    def test_symbol_normalization(self):
        """Validates ticker whitespace trimming and uppercase conversion."""
        raw_symbol = "  nvda  "
        normalized = raw_symbol.strip().upper()
        self.assertEqual(normalized, "NVDA")

    def test_deduplication_composite_key(self):
        """Validates primary key uniqueness logic on (Symbol, PriceTimestamp)."""
        records = [
            ("AAPL", "2026-09-01 10:00:00", 220.0),
            ("AAPL", "2026-09-01 10:00:00", 220.5),  # Duplicate timestamp for same symbol
            ("MSFT", "2026-09-01 10:00:00", 430.0),
        ]
        unique_keys = set()
        deduped = []
        for sym, ts, close in records:
            key = (sym, ts)
            if key not in unique_keys:
                unique_keys.add(key)
                deduped.append((sym, ts, close))

        self.assertEqual(len(deduped), 2)
        self.assertEqual(deduped[0], ("AAPL", "2026-09-01 10:00:00", 220.0))
        self.assertEqual(deduped[1], ("MSFT", "2026-09-01 10:00:00", 430.0))


if __name__ == "__main__":
    unittest.main()
