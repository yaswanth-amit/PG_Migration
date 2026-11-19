"""pytest: make the project folder importable (main.py, ui_app.py and the pipeline package)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
