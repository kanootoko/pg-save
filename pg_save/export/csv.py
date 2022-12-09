from typing import TextIO

import numpy as np
import pandas as pd
from loguru import logger


def to_csv(df: pd.DataFrame, filename_or_buf: str | TextIO) -> None:
    logger.debug("Saving csv" + f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else "")
    df = df.copy()
    for i in range(df.shape[1]):
        df.iloc[:, i] = pd.Series(
            map(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x, df.iloc[:, i]), dtype=object
        )
    df = df.replace({np.nan: None})
    logger.debug("Saving csv" + (f" to {filename_or_buf}" if isinstance(filename_or_buf, str) else ""))
    df.to_csv(filename_or_buf, header=True, index=False)
    logger.debug("Saved")
