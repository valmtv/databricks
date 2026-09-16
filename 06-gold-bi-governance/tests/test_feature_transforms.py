"""
Unit Tests: Feature Engineering & Surrogate Keys
------------------------------------------------
Validates surrogate key hashing determinism, collision properties,
WHO PM2.5 guideline exceedance logic, and health severity scores.
"""

import unittest
from air_quality_transforms.features import (
    calculate_surrogate_key,
    compute_who_pm25_exceedance,
    calculate_health_severity_score,
)


class TestFeatureTransforms(unittest.TestCase):

    def test_surrogate_key_determinism(self):
        """Same input tokens must always generate the exact same positive integer."""
        key1 = calculate_surrogate_key("US-NYC-001", "2026-09-01 10:00:00")
        key2 = calculate_surrogate_key("US-NYC-001", "2026-09-01 10:00:00")
        self.assertEqual(key1, key2)
        self.assertIsInstance(key1, int)
        self.assertGreater(key1, 0)

    def test_surrogate_key_uniqueness_across_grains(self):
        """Different inputs must produce different surrogate keys."""
        k1 = calculate_surrogate_key("US-NYC-001", "2026-09-01 10:00:00")
        k2 = calculate_surrogate_key("US-NYC-001", "2026-09-01 11:00:00")
        k3 = calculate_surrogate_key("GB-LON-001", "2026-09-01 10:00:00")
        self.assertNotEqual(k1, k2)
        self.assertNotEqual(k1, k3)
        self.assertNotEqual(k2, k3)

    def test_who_pm25_exceedance_boundary(self):
        """WHO daily guideline threshold is 15.0 ug/m3."""
        self.assertFalse(compute_who_pm25_exceedance(15.0))     # Boundary: exactly 15.0 is not exceeded
        self.assertTrue(compute_who_pm25_exceedance(15.01))    # Exceeded
        self.assertTrue(compute_who_pm25_exceedance(35.5))     # Exceeded
        self.assertFalse(compute_who_pm25_exceedance(12.4))    # Below
        self.assertFalse(compute_who_pm25_exceedance(0.0))     # Zero
        self.assertFalse(compute_who_pm25_exceedance(None))    # None handled gracefully

    def test_health_severity_score_mapping(self):
        """Continuous health severity metric scale from 1.0 to 6.0."""
        self.assertEqual(calculate_health_severity_score(25), 1.0)
        self.assertEqual(calculate_health_severity_score(50), 1.0)
        self.assertEqual(calculate_health_severity_score(75), 2.0)
        self.assertEqual(calculate_health_severity_score(100), 2.0)
        self.assertEqual(calculate_health_severity_score(125), 3.0)
        self.assertEqual(calculate_health_severity_score(150), 3.0)
        self.assertEqual(calculate_health_severity_score(180), 4.0)
        self.assertEqual(calculate_health_severity_score(200), 4.0)
        self.assertEqual(calculate_health_severity_score(250), 5.0)
        self.assertEqual(calculate_health_severity_score(300), 5.0)
        self.assertEqual(calculate_health_severity_score(450), 6.0)
        self.assertEqual(calculate_health_severity_score(None), 0.0)


if __name__ == "__main__":
    unittest.main()
