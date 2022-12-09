import json
from typing import Any, TextIO

import numpy as np
import pandas as pd
from loguru import logger

from pg_save.utils import NpEncoder


def to_json(df: pd.DataFrame, filename_or_buf: str | TextIO) -> None:
    logger.debug(f"Saving json" + f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else "")
    df = df.copy()

    serializable_types = ["object", "int64", "float64", "bool"]

    for col in set(df.columns):
        if isinstance(df[col], pd.DataFrame):
            logger.warning(f'Table has more than one column with the same name: "{col}", renaming')
            r = iter(range(df.shape[1] + 1))
            df = df.rename(lambda name: name if name != col else f"{col}_{next(r)}", axis=1)
            for col_idx in range(next(r)):
                if df[f"{col}_{col_idx}"].dtypes not in serializable_types:
                    logger.warning(f'Dropping non-serializable "{col}_{col_idx}" column')
        else:
            if df[col].dtypes not in serializable_types:
                logger.warning(f'Dropping non-serializable "{col}" column')
                df = df.drop(col, axis=1)
    for i in range(df.shape[1]):
        df.iloc[:, i] = pd.Series(
            list(map(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x, df.iloc[:, i])), dtype=object
        )
    df = df.replace({np.nan: None})
    data: list[dict[str, Any]] = [dict(row) for _, row in df.iterrows()]
    if isinstance(filename_or_buf, str):
        with open(filename_or_buf, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4, cls=NpEncoder)
    else:
        json.dump(data, filename_or_buf, ensure_ascii=False, cls=NpEncoder)
    logger.debug("Saved")
