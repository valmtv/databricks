"""
Tests Package Initialization
----------------------------
Ensures that the parent folder (containing air_quality_transforms) is automatically
present in sys.path regardless of whether tests are discovered from the repository root,
from within the folder, via pytest, or via python -m unittest.
"""

import sys
import os

PACKAGE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)
