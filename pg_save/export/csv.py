"""
Logic of exporting pandas DataFrame to csv is defined here.
"""
from typing import TextIO

import numpy as np
import pandas as pd
from loguru import logger


def to_csv(dataframe: pd.DataFrame, filename_or_buf: str | TextIO) -> None:
    """Export pandas DataFrame to csv format.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        filename_or_buf (str | TextIO): filename or StringIO buffer.
    """
    logger.debug("Saving csv" + f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else "")

    dataframe = dataframe.copy()
    for i in range(dataframe.shape[1]):
        dataframe.iloc[:, i] = pd.Series(
            map(
                lambda x: int(x) if isinstance(x, float) and x.is_integer() else x,
                dataframe.iloc[:, i],
            ),
            dtype=object,
        )
    dataframe = dataframe.replace({np.nan: None})
    dataframe.to_csv(filename_or_buf, header=True, index=False)

    logger.debug("Saved")
