import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
print(BASE_DIR)
ENV_DIR = BASE_DIR / ".env"

load_dotenv(ENV_DIR)

API_KEY = os.getenv("API_KEY")

BRONZE_DATA_DIR = BASE_DIR / "data" / "bronze"