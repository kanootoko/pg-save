"""Pandas helpers are defined here."""

from collections import Counter, defaultdict

import numpy as np
import pandas as pd


def beautify_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Reset index if it was changed (without delition of an old index), rename duplicating columns names,
    cast float values to integer where possible without precision loss, replace NaN values with python None.

    Returns copy of a given DataFrame without modification.
    """
    dataframe = dataframe.copy()
    if not isinstance(dataframe.index, pd.RangeIndex) or not all(dataframe.index == pd.RangeIndex(dataframe.shape[0])):
        dataframe = dataframe.reset_index()

    columns_name_counts = Counter(dataframe.columns)
    counters = defaultdict(lambda: -1)

    def rename_func(column_name: str) -> str:
        while columns_name_counts[column_name] > 1:
            counters[column_name] += 1
            column_name = f"{column_name}_{counters[column_name]}"
            columns_name_counts[column_name] += 1
        return column_name

    dataframe = dataframe.rename(rename_func, axis=1)

    for column_name in dataframe.columns:
        dataframe[column_name] = pd.Series(
            map(
                lambda x: int(x) if isinstance(x, float) and x.is_integer() else x,
                dataframe[column_name],
            ),
            dtype=object,
        )
    dataframe = dataframe.replace({np.nan: None})

    return dataframe
