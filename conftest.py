"""Root conftest: add the repo root to sys.path so `src.*` imports resolve."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
