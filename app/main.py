import os
import sys

# Ensure the root project directory is on sys.path so root modules and relative imports resolve
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from main import app  # noqa: E402, F401

__all__ = ["app"]
