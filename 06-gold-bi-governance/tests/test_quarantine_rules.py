"""
Unit Tests: Quarantine Rules & Data Quality Routing
---------------------------------------------------
Validates the Enterprise Quarantine Table Pattern:
- Corrupted or non-compliant records are classified for quarantine
- Diagnostic failure codes are generated accurately
- Valid records bypass quarantine to clean tables
- Invariant: Total Incoming = Valid Count + Quarantine Count
"""

import unittest
from datetime import datetime, timezone, timedelta
from air_quality_transforms.quarantine import (
    evaluate_quarantine_conditions,
    split_valid_and_quarantined,
    QUARANTINE_RULES,
)


class TestQuarantineRules(unittest.TestCase):

    def setUp(self):
        self.valid_record = {
            "event_id": "EVT-NYC-001",
            "station_id": "US-NYC-001",
            "city": "New York",
            "country": "USA",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "recorded_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "pm2_5": 12.5,
            "pm10": 22.0,
            "us_aqi": 52
        }

    def test_valid_record_passes_quarantine(self):
        """Clean valid records must not be quarantined."""
        is_quarantine, reasons = evaluate_quarantine_conditions(self.valid_record)
        self.assertFalse(is_quarantine)
        self.assertEqual(len(reasons), 0)

    def test_missing_mandatory_keys_quarantine(self):
        """Missing or empty event_id, station_id, or city triggers quarantine."""
        rec_no_event = dict(self.valid_record, event_id=None)
        is_q, reasons = evaluate_quarantine_conditions(rec_no_event)
        self.assertTrue(is_q)
        self.assertIn("MISSING_EVENT_ID", reasons)

        rec_empty_station = dict(self.valid_record, station_id="  ")
        is_q, reasons = evaluate_quarantine_conditions(rec_empty_station)
        self.assertTrue(is_q)
        self.assertIn("MISSING_STATION_ID", reasons)

        rec_no_city = dict(self.valid_record, city="")
        is_q, reasons = evaluate_quarantine_conditions(rec_no_city)
        self.assertTrue(is_q)
        self.assertIn("MISSING_CITY", reasons)

    def test_invalid_timestamp_quarantine(self):
        """Missing or future-dated timestamps trigger quarantine."""
        rec_null_ts = dict(self.valid_record, recorded_at=None)
        is_q, reasons = evaluate_quarantine_conditions(rec_null_ts)
        self.assertTrue(is_q)
        self.assertIn("NULL_RECORDED_TIMESTAMP", reasons)

        future_ts = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        rec_future_ts = dict(self.valid_record, recorded_at=future_ts)
        is_q, reasons = evaluate_quarantine_conditions(rec_future_ts)
        self.assertTrue(is_q)
        self.assertIn("FUTURE_RECORDED_TIMESTAMP", reasons)

    def test_out_of_bounds_coordinates_quarantine(self):
        """Latitude outside [-90..90] or Longitude outside [-180..180] triggers quarantine."""
        rec_bad_lat = dict(self.valid_record, latitude=95.0)
        is_q, reasons = evaluate_quarantine_conditions(rec_bad_lat)
        self.assertTrue(is_q)
        self.assertIn("LATITUDE_OUT_OF_BOUNDS", reasons)

        rec_bad_lon = dict(self.valid_record, longitude=-195.0)
        is_q, reasons = evaluate_quarantine_conditions(rec_bad_lon)
        self.assertTrue(is_q)
        self.assertIn("LONGITUDE_OUT_OF_BOUNDS", reasons)

    def test_negative_pollutants_and_invalid_aqi_quarantine(self):
        """Physical sensor faults (negative concentrations, AQI > 500) trigger quarantine."""
        rec_neg_pm = dict(self.valid_record, pm2_5=-10.0)
        is_q, reasons = evaluate_quarantine_conditions(rec_neg_pm)
        self.assertTrue(is_q)
        self.assertIn("NEGATIVE_PM25", reasons)

        rec_bad_aqi = dict(self.valid_record, us_aqi=750)
        is_q, reasons = evaluate_quarantine_conditions(rec_bad_aqi)
        self.assertTrue(is_q)
        self.assertIn("AQI_OUT_OF_BOUNDS", reasons)

    def test_multi_defect_diagnostic_tagging(self):
        """Simultaneous defects are all captured in the diagnostic reason list."""
        multi_defect = dict(
            self.valid_record,
            station_id=None,
            pm2_5=-5.0,
            latitude=150.0,
            us_aqi=999
        )
        is_q, reasons = evaluate_quarantine_conditions(multi_defect)
        self.assertTrue(is_q)
        self.assertIn("MISSING_STATION_ID", reasons)
        self.assertIn("NEGATIVE_PM25", reasons)
        self.assertIn("LATITUDE_OUT_OF_BOUNDS", reasons)
        self.assertIn("AQI_OUT_OF_BOUNDS", reasons)

    def test_batch_split_conservation_invariant(self):
        """
        Tests splitting a batch of records:
        Asserts the Zero Silent Loss invariant: Total Batch Count = Valid Count + Quarantine Count.
        """
        batch = [
            self.valid_record,
            dict(self.valid_record, event_id="EVT-002", city="London"),
            dict(self.valid_record, event_id="EVT-003", pm2_5=-1.0),      # Invalid
            dict(self.valid_record, event_id="EVT-004", station_id=None),  # Invalid
            dict(self.valid_record, event_id="EVT-005", latitude=99.0),    # Invalid
            dict(self.valid_record, event_id="EVT-006", city="Tokyo"),
        ]

        valid, quarantined = split_valid_and_quarantined(batch)

        self.assertEqual(len(batch), len(valid) + len(quarantined))
        self.assertEqual(len(valid), 3)
        self.assertEqual(len(quarantined), 3)

        # Quarantined records must have diagnostic metadata attached
        for q in quarantined:
            self.assertIn("quarantine_reason", q)
            self.assertIn("quarantined_at", q)
            self.assertTrue(len(q["quarantine_reason"]) > 0)


if __name__ == "__main__":
    unittest.main()
