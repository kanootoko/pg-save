"""Logic of exporting pandas DataFrame to excel is defined here."""

from __future__ import annotations

from typing import BinaryIO

import pandas as pd
from loguru import logger


def to_excel(dataframe: pd.DataFrame, filename_or_buf: str | BinaryIO) -> None:
    """Export pandas DataFrame to excel format.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        filename_or_buf (str | BinaryIO): filename or BytesIO buffer.
    """
    logger.debug("Saving excel" + (f" to {filename_or_buf}" if isinstance(filename_or_buf, str) else ""))

    if isinstance(filename_or_buf, str):
        dataframe.to_excel(filename_or_buf, header=True, index=False)
    else:
        with pd.ExcelWriter(filename_or_buf) as writer:  # pylint: disable=abstract-class-instantiated
            dataframe.to_excel(writer, header=True, index=False)

    logger.debug("Saved")
