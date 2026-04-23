from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PARENT = PROJECT_ROOT.parent
INCOMING_DATA_DIR = PROJECT_PARENT / "Incoming fleet data"
AWS_DIR = PROJECT_ROOT / "aws"
ROOT_ENV_FILE = PROJECT_ROOT / ".env"
AWS_ENV_FILE = AWS_DIR / ".env"
