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
    # Databricks kernel executes scripts via exec(compile(...)) where __file__ is undefined
    script_path = globals().get("__file__") or (sys.argv[0] if sys.argv and sys.argv[0] else None)
    if script_path:
        base_dir = os.path.dirname(os.path.abspath(script_path))
    else:
        base_dir = os.getcwd()

    tests_dir = os.path.join(base_dir, "tests")
    if not os.path.isdir(tests_dir):
        tests_dir = os.path.abspath("tests")

    print(f"[TEST-RUNNER] Discovering tests in: {tests_dir}")
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=tests_dir, pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2 if args.verbose else 1)
    result = runner.run(suite)

    if not result.wasSuccessful():
        raise RuntimeError(f"Tests failed: {len(result.failures)} failures, {len(result.errors)} errors")
    print("[TEST-RUNNER] All tests passed successfully!")


if __name__ == "__main__":
    main()
