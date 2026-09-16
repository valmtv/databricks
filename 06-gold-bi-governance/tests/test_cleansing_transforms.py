"""
Unit Tests: Cleansing & Schema Enforcement
------------------------------------------
Tests coordinate boundary checks, pollutant range validations,
timestamp freshness checks, and type casting.
"""

import unittest
from datetime import datetime, timezone, timedelta
from air_quality_transforms.cleansing import (
    validate_coordinate_bounds,
    validate_pollutant_ranges,
    validate_timestamp_freshness,
    clean_air_quality_records,
    cast_pollutant_metrics,
)


class TestCleansingTransforms(unittest.TestCase):

    def test_coordinate_valid_bounds(self):
        """Earth coordinates must fall within [-90..90, -180..180]."""
        self.assertTrue(validate_coordinate_bounds(40.7128, -74.0060))    # NYC
        self.assertTrue(validate_coordinate_bounds(51.5074, -0.1278))     # London
        self.assertTrue(validate_coordinate_bounds(35.6762, 139.6503))    # Tokyo
        self.assertTrue(validate_coordinate_bounds(90.0, 0.0))            # North pole
        self.assertTrue(validate_coordinate_bounds(-90.0, 0.0))           # South pole
        self.assertTrue(validate_coordinate_bounds(0.0, 180.0))           # Date line east
        self.assertTrue(validate_coordinate_bounds(0.0, -180.0))          # Date line west

    def test_coordinate_out_of_bounds(self):
        """Coordinates outside physical limits must return False."""
        self.assertFalse(validate_coordinate_bounds(90.0001, 10.0))        # Latitude > 90
        self.assertFalse(validate_coordinate_bounds(-90.1, 10.0))          # Latitude < -90
        self.assertFalse(validate_coordinate_bounds(45.0, 180.001))        # Longitude > 180
        self.assertFalse(validate_coordinate_bounds(45.0, -180.5))         # Longitude < -180
        self.assertFalse(validate_coordinate_bounds(None, -74.0))          # Null latitude
        self.assertFalse(validate_coordinate_bounds(40.7, None))           # Null longitude
        self.assertFalse(validate_coordinate_bounds("invalid", 10.0))      # Corrupted string

    def test_pollutant_range_valid(self):
        """Pollutant values must be non-negative, and AQI must be [0..500]."""
        self.assertTrue(validate_pollutant_ranges(pm2_5=12.5, pm10=25.0, us_aqi=50))
        self.assertTrue(validate_pollutant_ranges(pm2_5=0.0, pm10=0.0, us_aqi=0))
        self.assertTrue(validate_pollutant_ranges(pm2_5=250.0, pm10=400.0, us_aqi=500))
        # Null values are permitted for unmetered stations
        self.assertTrue(validate_pollutant_ranges(pm2_5=None, pm10=20.0, us_aqi=45))
        self.assertTrue(validate_pollutant_ranges(pm2_5=10.0, pm10=None, us_aqi=None))

    def test_pollutant_range_invalid(self):
        """Negative pollutant values or out-of-scale AQI must be flagged."""
        self.assertFalse(validate_pollutant_ranges(pm2_5=-1.0, pm10=20.0, us_aqi=50))
        self.assertFalse(validate_pollutant_ranges(pm2_5=10.0, pm10=-5.0, us_aqi=50))
        self.assertFalse(validate_pollutant_ranges(pm2_5=10.0, pm10=20.0, us_aqi=-1))
        self.assertFalse(validate_pollutant_ranges(pm2_5=10.0, pm10=20.0, us_aqi=501))
        self.assertFalse(validate_pollutant_ranges(pm2_5=10.0, pm10=20.0, us_aqi=9999))

    def test_timestamp_freshness(self):
        """Checks temporal validity and clock-skew tolerance."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(hours=2)
        slight_future = now + timedelta(seconds=60)      # Within 300s skew tolerance
        distant_future = now + timedelta(days=2)        # Anomaly

        self.assertTrue(validate_timestamp_freshness(past_time, reference_time=now))
        self.assertTrue(validate_timestamp_freshness(slight_future, reference_time=now))
        self.assertFalse(validate_timestamp_freshness(distant_future, reference_time=now))
        self.assertFalse(validate_timestamp_freshness(None, reference_time=now))
        self.assertFalse(validate_timestamp_freshness("not-a-date", reference_time=now))

    def test_clean_air_quality_records(self):
        """Validates key whitespace trimming and safe numeric casting."""
        raw = {
            "event_id": "  EVT-1001  ",
            "station_id": " US-NYC-001 ",
            "city": " New York ",
            "country": " USA ",
            "latitude": "40.7128",
            "longitude": "-74.0060",
            "pm2_5": "12.4",
            "us_aqi": "52",
            "corrupt_field": "ignore"
        }
        cleaned = clean_air_quality_records(raw)
        self.assertEqual(cleaned["event_id"], "EVT-1001")
        self.assertEqual(cleaned["station_id"], "US-NYC-001")
        self.assertEqual(cleaned["city"], "New York")
        self.assertEqual(cleaned["country"], "USA")
        self.assertEqual(cleaned["latitude"], 40.7128)
        self.assertEqual(cleaned["longitude"], -74.0060)
        self.assertEqual(cleaned["pm2_5"], 12.4)
        self.assertEqual(cleaned["us_aqi"], 52)


if __name__ == "__main__":
    unittest.main()
