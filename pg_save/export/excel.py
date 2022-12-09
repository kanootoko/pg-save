from typing import BinaryIO

import numpy as np
import pandas as pd
from loguru import logger


def to_excel(df: pd.DataFrame, filename_or_buf: str | BinaryIO) -> None:
    logger.debug('Saving excel' + (f' to {filename_or_buf}' if isinstance(filename_or_buf, str) else ''))
    df = df.copy()
    for i in range(df.shape[1]):
        df.iloc[:, i] = pd.Series(map(lambda x: int(x) if isinstance(x, float) and x.is_integer() else x, df.iloc[:, i]), dtype=object)
    df = df.replace({np.nan: None})
    if isinstance(filename_or_buf, str):
        df.to_excel(filename_or_buf, header=True, index=False)
    else:
        with pd.ExcelWriter(filename_or_buf) as writer:
            df.to_excel(writer, header=True, index=False)
    logger.debug('Saved')