# conftest.py  (project root)
# ─────────────────────────────────────────────────────────────────────────────
# Ensures that the project root is on sys.path so all first-party packages
# (agents/, models/, orchestrator/) are importable from any test module
# without needing a package install.
# ─────────────────────────────────────────────────────────────────────────────
import sys
from pathlib import Path

# Insert the project root (the directory containing this file) at position 0
# so it takes precedence over any installed packages with the same name.
ROOT = Path(__file__).parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
