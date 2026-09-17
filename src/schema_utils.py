import json
from pathlib import Path

from fastavro import parse_schema


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = PROJECT_ROOT / "schemas"


def load_schema(filename: str):
    schema_path = SCHEMA_DIR / filename

    with open(schema_path, "r", encoding="utf-8") as file:
        schema = json.load(file)

    return parse_schema(schema)