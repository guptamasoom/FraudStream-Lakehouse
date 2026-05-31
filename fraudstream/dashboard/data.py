from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from fraudstream.pipeline.utils.io import read_table

GOLD_ROOT = os.getenv("GOLD_DATA_PATH", "data/gold")


def gold_table(name: str) -> pd.DataFrame:
    return read_table(Path(GOLD_ROOT) / name)
