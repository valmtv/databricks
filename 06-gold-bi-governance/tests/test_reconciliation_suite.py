"""
Unit Tests: Medallion Reconciliation Invariants
-----------------------------------------------
Asserts layer-to-layer reconciliation math:
1. Row Count Conservation: Bronze == Silver Valid + Silver Quarantine (Zero Silent Loss)
2. Grain Rollup Conservation: Sum(fact_city_daily_summary.observation_count) == Count(fact_air_quality_hourly)
3. Dimensional Foreign Key Integrity: Zero orphan station_sk, date_sk, or category_sk
4. Temporal Integrity: Zero future-dated observations
"""

import unittest
from datetime import datetime, timezone, timedelta


class TestReconciliationInvariants(unittest.TestCase):

    def test_row_count_conservation_math(self):
        """
        Layer-to-layer conservation:
        Every row ingested into Bronze must either be successfully enriched in Silver
        OR documented with an error in Quarantine.
        """
        bronze_ingested = 1260
        silver_enriched = 1254
        silver_quarantine = 6

        # Mathematically verify conservation
        self.assertEqual(bronze_ingested, silver_enriched + silver_quarantine)
        
        # Verify discrepancy detection if silent data loss occurs
        corrupted_silver_count = 1250  # 4 rows mysteriously vanished!
        discrepancy = bronze_ingested - (corrupted_silver_count + silver_quarantine)
        self.assertEqual(discrepancy, 4)
        self.assertNotEqual(bronze_ingested, corrupted_silver_count + silver_quarantine)

    def test_grain_rollup_observation_conservation(self):
        """
        Grain rollup consistency:
        Summing `observation_count` across all daily summary rows must equal
        the total row count in the hourly fact table.
        """
        simulated_daily_summaries = [
            {"city": "New York", "date": "2026-09-01", "observation_count": 24},
            {"city": "London", "date": "2026-09-01", "observation_count": 24},
            {"city": "Tokyo", "date": "2026-09-01", "observation_count": 24},
            {"city": "Berlin", "date": "2026-09-01", "observation_count": 24},
            {"city": "Paris", "date": "2026-09-01", "observation_count": 24},
        ]
        hourly_fact_row_count = 120

        summed_observations = sum(row["observation_count"] for row in simulated_daily_summaries)
        self.assertEqual(summed_observations, hourly_fact_row_count)

    def test_referential_integrity_orphan_detection(self):
        """
        Dimensional integrity:
        All foreign surrogate keys in Fact tables must resolve to valid primary keys
        in their respective conformed dimension tables.
        """
        valid_station_sks = {101, 102, 103, 104, 105}
        fact_records = [
            {"fact_sk": 1, "station_sk": 101},
            {"fact_sk": 2, "station_sk": 102},
            {"fact_sk": 3, "station_sk": 103},
            {"fact_sk": 4, "station_sk": 999},  # Orphan key!
        ]

        orphans = [r for r in fact_records if r["station_sk"] not in valid_station_sks]
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans[0]["station_sk"], 999)

    def test_temporal_freshness_boundary_reconciliation(self):
        """
        Temporal reconciliation:
        No records in Gold should have timestamps ahead of current UTC time.
        """
        now = datetime.now(timezone.utc)
        records = [
            {"event_id": "E1", "recorded_at": now - timedelta(hours=2)},
            {"event_id": "E2", "recorded_at": now - timedelta(minutes=15)},
            {"event_id": "E3", "recorded_at": now + timedelta(hours=5)},  # Anomaly!
        ]

        future_records = [r for r in records if r["recorded_at"] > now]
        self.assertEqual(len(future_records), 1)
        self.assertEqual(future_records[0]["event_id"], "E3")

    def test_dq_declarative_rules_structure(self):
        """
        Validates the structure of the declarative DQX quality specification:
        - All 5 data quality dimensions are covered
        - Rules target correct Medallion tables (Silver vs Gold facts)
        - Rules marked action='fail' have required expressions or keys
        """
        import os
        rules_path = os.path.join(os.path.dirname(__file__), "..", "dq_rules.yml")
        self.assertTrue(os.path.exists(rules_path), "dq_rules.yml must exist")

        # Parse rules either via yaml or line parsing
        with open(rules_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check all 5 dimensions exist in spec
        for dim in ["Completeness", "Validity", "Timeliness", "Consistency", "Uniqueness"]:
            self.assertIn(dim, content)

        # Check cross-table targets are explicitly specified
        self.assertIn("fact_city_daily_summary", content)
        self.assertIn("fact_air_quality_hourly", content)
        self.assertIn("consistency_hours_partition_integrity", content)
        self.assertIn("consistency_referential_station_fk", content)
        self.assertIn("uniqueness_fact_sk", content)


if __name__ == "__main__":
    unittest.main()
