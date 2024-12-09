"""Logic of exporting pandas DataFrame to csv is defined here."""

from __future__ import annotations

from typing import TextIO

import pandas as pd
from loguru import logger


def to_csv(dataframe: pd.DataFrame, filename_or_buf: str | TextIO) -> None:
    """Export pandas DataFrame to csv format.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        filename_or_buf (str | TextIO): filename or StringIO buffer.
    """
    logger.debug("Saving csv" + (f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else ""))

    dataframe.to_csv(filename_or_buf, header=True, index=False)

    logger.debug("Saved")
