import json
from typing import TextIO

import numpy as np
import pandas as pd
from loguru import logger

from pg_save.export.default_crs import default_crs
from pg_save.utils import NpEncoder


def to_geojson(
    df: pd.DataFrame, filename_or_buf: str | TextIO, geometry_column: str = "geometry", crs: int = default_crs
) -> None:
    logger.debug("Saving geojson" + (f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else ""))
    serializable_types = ["object", "int64", "float64", "bool"]

    if geometry_column not in df.columns:
        logger.error(f'Geometry column "{geometry_column}" is not present, aborting')
        return

    geometry_series = df[geometry_column]
    df = df.drop(geometry_column, axis=1)

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

    geojson = {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
        "features": [
            {"type": "Feature", "properties": dict(row), "geometry": geometry}
            for (_, row), geometry in zip(df.iterrows(), geometry_series)
        ],
    }
    if isinstance(filename_or_buf, str):
        geojson["name"] = filename_or_buf
        with open(filename_or_buf, "w", encoding="utf-8") as file:
            json.dump(geojson, file, ensure_ascii=False, cls=NpEncoder)
    else:
        json.dump(geojson, filename_or_buf, ensure_ascii=False, cls=NpEncoder)

    logger.debug("Saved")
