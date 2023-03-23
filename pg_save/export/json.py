"""
Logic of exporting pandas DataFrame to json is defined here.
"""
import json
from typing import Any, TextIO

import numpy as np
import pandas as pd
from loguru import logger

from pg_save.utils import NpEncoder


def to_json(dataframe: pd.DataFrame, filename_or_buf: str | TextIO) -> None:
    """Export pandas DataFrame to json format.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        filename_or_buf (str | TextIO): filename or StringIO buffer.
    """
    logger.debug("Saving json" + f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else "")
    dataframe = dataframe.copy()

    serializable_types = ["object", "int64", "float64", "bool"]

    for col in set(dataframe.columns):
        if isinstance(dataframe[col], pd.DataFrame):
            logger.warning(f'Table has more than one column with the same name: "{col}", renaming')
            overlapping_columns_number_range = iter(range(dataframe.shape[1] + 1))
            dataframe = dataframe.rename(
                lambda name, col=col, rng=overlapping_columns_number_range: name
                if name != col
                else f"{col}_{next(rng)}",
                axis=1,
            )
            for col_idx in range(next(overlapping_columns_number_range)):
                if dataframe[f"{col}_{col_idx}"].dtypes not in serializable_types:
                    logger.warning(f'Dropping non-serializable "{col}_{col_idx}" column')
        else:
            if dataframe[col].dtypes not in serializable_types:
                logger.warning(f'Dropping non-serializable "{col}" column')
                dataframe = dataframe.drop(col, axis=1)
    for i in range(dataframe.shape[1]):
        dataframe.iloc[:, i] = pd.Series(
            list(
                map(
                    lambda x: int(x) if isinstance(x, float) and x.is_integer() else x,
                    dataframe.iloc[:, i],
                )
            ),
            dtype=object,
        )
    dataframe = dataframe.replace({np.nan: None})
    data: list[dict[str, Any]] = [dict(row) for _, row in dataframe.iterrows()]
    if isinstance(filename_or_buf, str):
        with open(filename_or_buf, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4, cls=NpEncoder)
    else:
        json.dump(data, filename_or_buf, ensure_ascii=False, cls=NpEncoder)

    logger.debug("Saved")
