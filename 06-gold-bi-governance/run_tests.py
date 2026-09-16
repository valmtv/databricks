#!/usr/bin/env python3
"""
Test Runner: Air Quality Medallion Suite
----------------------------------------
Executes the comprehensive transformation and data quality unit test suite.

USAGE:
  python3 run_tests.py            # Local execution (default, ultra-fast)
  python3 run_tests.py --connect  # Remote execution via Databricks Connect
  python3 run_tests.py -v         # Verbose output
"""

import sys
import os
import unittest
import argparse


def main():
    parser = argparse.ArgumentParser(description="Run Air Quality Medallion Unit Tests")
    parser.add_argument("--connect", action="store_true", help="Enable Databricks Connect remote execution")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose test results")
    args = parser.parse_args()

    if args.connect:
        print("[TEST-RUNNER] Databricks Connect mode requested.")
        os.environ["DATABRICKS_CONNECT"] = "1"
    else:
        print("[TEST-RUNNER] Running in local pure-python / PySpark mode.")

    tests_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tests")
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=tests_dir, pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2 if args.verbose or True else 1)
    result = runner.run(suite)

    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
