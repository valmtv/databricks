"""
Unit Tests: Aggregation & Summary Transforms
--------------------------------------------
Validates daily compliance grading, unhealthy hours percentage calculation,
and aggregate consistency invariants.
"""

import unittest
import sys
import os

# Ensure parent directory is in sys.path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from air_quality_transforms.aggregations import (

    assign_compliance_grade,
    calculate_unhealthy_hours_percentage,
    calculate_daily_aggregates,
)


class TestAggregationTransforms(unittest.TestCase):

    def test_compliance_grade_tiers(self):
        """Validates environmental compliance tier boundaries."""
        # Grade A (Clean): <= 50
        self.assertEqual(assign_compliance_grade(25.0), "Grade A (Clean)")
        self.assertEqual(assign_compliance_grade(50.0), "Grade A (Clean)")

        # Grade B (Acceptable): <= 100
        self.assertEqual(assign_compliance_grade(50.1), "Grade B (Acceptable)")
        self.assertEqual(assign_compliance_grade(75.0), "Grade B (Acceptable)")
        self.assertEqual(assign_compliance_grade(100.0), "Grade B (Acceptable)")

        # Grade C (Warning): <= 150
        self.assertEqual(assign_compliance_grade(100.1), "Grade C (Warning)")
        self.assertEqual(assign_compliance_grade(125.0), "Grade C (Warning)")
        self.assertEqual(assign_compliance_grade(150.0), "Grade C (Warning)")

        # Grade D (Action Required): > 150
        self.assertEqual(assign_compliance_grade(150.1), "Grade D (Action Required)")
        self.assertEqual(assign_compliance_grade(250.0), "Grade D (Action Required)")
        self.assertEqual(assign_compliance_grade(None), "Grade D (Action Required)")

    def test_unhealthy_hours_percentage(self):
        """Tests percentage calculation and zero division protection."""
        self.assertEqual(calculate_unhealthy_hours_percentage(6, 24), 25.0)
        self.assertEqual(calculate_unhealthy_hours_percentage(0, 24), 0.0)
        self.assertEqual(calculate_unhealthy_hours_percentage(24, 24), 100.0)
        self.assertEqual(calculate_unhealthy_hours_percentage(12, 24), 50.0)
        # Zero division guard
        self.assertEqual(calculate_unhealthy_hours_percentage(0, 0), 0.0)

    def test_daily_summary_aggregation_invariant(self):
        """
        Tests aggregate roll-up from 24 hourly records:
        Asserts the invariant: hours_safe + hours_moderate + hours_unhealthy == observation_count.
        """
        simulated_day = []
        for hour in range(24):
            # 10 safe hours (AQI 40), 8 moderate hours (AQI 80), 6 unhealthy hours (AQI 140)
            if hour < 10:
                aqi = 40
            elif hour < 18:
                aqi = 80
            else:
                aqi = 140
            simulated_day.append({"hour": hour, "us_aqi": aqi})

        summary = calculate_daily_aggregates(simulated_day)

        self.assertEqual(summary["observation_count"], 24)
        self.assertEqual(summary["hours_safe"], 10)
        self.assertEqual(summary["hours_moderate"], 8)
        self.assertEqual(summary["hours_unhealthy"], 6)
        
        # INVARIANT: Partition of hours must sum exactly to total observations
        total_partitioned = summary["hours_safe"] + summary["hours_moderate"] + summary["hours_unhealthy"]
        self.assertEqual(total_partitioned, summary["observation_count"])

        # Unhealthy hours percentage: 6 / 24 = 25.0%
        self.assertEqual(summary["unhealthy_hours_pct"], 25.0)

        # Average AQI: (10*40 + 8*80 + 6*140) / 24 = (400 + 640 + 840) / 24 = 1880 / 24 = 78.33... -> 78.3
        self.assertEqual(summary["avg_us_aqi"], 78.3)
        self.assertEqual(summary["compliance_grade"], "Grade B (Acceptable)")


if __name__ == "__main__":
    unittest.main()
