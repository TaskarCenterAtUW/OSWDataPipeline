import json
from pathlib import Path

from ...constants import TMP_DIR


with open(Path(Path(__file__).parent, "ned_13_index.json")) as f:
    ned_13_index = json.load(f)["tiles"]
