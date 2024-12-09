"""Logic of exporting pandas DataFrame to json is defined here."""

from __future__ import annotations

import json
from typing import Any, TextIO

import pandas as pd
from loguru import logger

from pg_save.utils import NpEncoder
from pg_save.utils.pd import beautify_dataframe


def to_json(dataframe: pd.DataFrame, filename_or_buf: str | TextIO) -> None:
    """Export pandas DataFrame to json format.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        filename_or_buf (str | TextIO): filename or StringIO buffer.
    """
    logger.debug("Saving json" + f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else "")
    dataframe = dataframe.copy()
    if not isinstance(dataframe.index, pd.RangeIndex) or not all(dataframe.index == pd.RangeIndex(dataframe.shape[0])):
        dataframe = dataframe.reset_index()

    serializable_types = ["object", "int64", "float64", "bool"]
    dataframe = beautify_dataframe(dataframe)

    for col in set(dataframe.columns):
        if dataframe[col].dtypes not in serializable_types:
            logger.warning(f'Dropping non-serializable "{col}" column')
            dataframe = dataframe.drop(col, axis=1)

    data: list[dict[str, Any]] = [dict(row) for _, row in dataframe.iterrows()]
    if isinstance(filename_or_buf, str):
        with open(filename_or_buf, "w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=4, cls=NpEncoder)
    else:
        json.dump(data, filename_or_buf, ensure_ascii=False, cls=NpEncoder)

    logger.debug("Saved")
