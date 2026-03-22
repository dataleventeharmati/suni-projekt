import pandas as pd
from pathlib import Path
import sys

sys.path.append(str(Path("src").resolve()))

from validate_and_clean import normalize_dates

def test_normalize_dates_basic():
    s = pd.Series(["2025/1/3", "45734", "", None])

    out = normalize_dates(s)

    assert out.iloc[0] == "2025-01-03"
    assert out.iloc[1] is not None
    assert out.iloc[2] is None
