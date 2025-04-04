from pathlib import Path

# ディレクトリパスの設定
ROOT = Path(".")
DATA_DIR = ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"

ENCODING_OPTIONS = ["utf8", "shift-jis", "cp932"]
NAME_PATTERNS = ["USER"]
