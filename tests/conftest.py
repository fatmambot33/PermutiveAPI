"""Global pytest fixtures and configuration."""

import sys
from pathlib import Path

# Ensure the package root is on the Python path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
