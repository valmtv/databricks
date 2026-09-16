"""
Unit Tests: Enrichment Transformations
--------------------------------------
Validates EPA AQI category classification, health advisory mapping,
and standard color code assignment across boundary intervals.
"""

import unittest
import sys
import os

# Ensure parent directory is in sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from air_quality_transforms.enrichment import (

    classify_aqi_category,
    get_color_code_for_aqi,
    EPA_AQI_TIERS,
)


class TestEnrichmentTransforms(unittest.TestCase):

    def test_aqi_category_boundaries(self):
        """Tests exact threshold boundaries for each EPA AQI category."""
        # Tier 1: Good (0-50)
        self.assertEqual(classify_aqi_category(0), "Good")
        self.assertEqual(classify_aqi_category(25), "Good")
        self.assertEqual(classify_aqi_category(50), "Good")

        # Tier 2: Moderate (51-100)
        self.assertEqual(classify_aqi_category(51), "Moderate")
        self.assertEqual(classify_aqi_category(75), "Moderate")
        self.assertEqual(classify_aqi_category(100), "Moderate")

        # Tier 3: Unhealthy for Sensitive Groups (101-150)
        self.assertEqual(classify_aqi_category(101), "Unhealthy for Sensitive Groups")
        self.assertEqual(classify_aqi_category(125), "Unhealthy for Sensitive Groups")
        self.assertEqual(classify_aqi_category(150), "Unhealthy for Sensitive Groups")

        # Tier 4: Unhealthy (151-200)
        self.assertEqual(classify_aqi_category(151), "Unhealthy")
        self.assertEqual(classify_aqi_category(180), "Unhealthy")
        self.assertEqual(classify_aqi_category(200), "Unhealthy")

        # Tier 5: Very Unhealthy (201-300)
        self.assertEqual(classify_aqi_category(201), "Very Unhealthy")
        self.assertEqual(classify_aqi_category(250), "Very Unhealthy")
        self.assertEqual(classify_aqi_category(300), "Very Unhealthy")

        # Tier 6: Hazardous (301-500)
        self.assertEqual(classify_aqi_category(301), "Hazardous")
        self.assertEqual(classify_aqi_category(400), "Hazardous")
        self.assertEqual(classify_aqi_category(500), "Hazardous")

    def test_aqi_category_unclassified_values(self):
        """Out of bounds or null AQI values must return 'Unknown' gracefully."""
        self.assertEqual(classify_aqi_category(None), "Unknown")
        self.assertEqual(classify_aqi_category(-1), "Unknown")
        self.assertEqual(classify_aqi_category(501), "Unknown")
        self.assertEqual(classify_aqi_category("corrupted"), "Unknown")

    def test_color_code_mapping(self):
        """Verifies correct standard EPA hex/name color mapping."""
        self.assertEqual(get_color_code_for_aqi(30), "Green")
        self.assertEqual(get_color_code_for_aqi(70), "Yellow")
        self.assertEqual(get_color_code_for_aqi(120), "Orange")
        self.assertEqual(get_color_code_for_aqi(175), "Red")
        self.assertEqual(get_color_code_for_aqi(250), "Purple")
        self.assertEqual(get_color_code_for_aqi(450), "Maroon")
        self.assertEqual(get_color_code_for_aqi(None), "Gray")
        self.assertEqual(get_color_code_for_aqi(999), "Gray")


if __name__ == "__main__":
    unittest.main()
