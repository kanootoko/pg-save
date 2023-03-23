"""
Logic of exporting pandas DataFrame to GeoJSON is defined here.
"""
import json
from typing import TextIO

import numpy as np
import pandas as pd
from loguru import logger

from pg_save.export.default_crs import DEFAULT_CRS
from pg_save.utils import NpEncoder


def to_geojson(
    dataframe: pd.DataFrame,
    filename_or_buf: str | TextIO,
    geometry_column: str = "geometry",
    crs: int | str = DEFAULT_CRS,
) -> None:
    """Export pandas DataFrame to GeoJSON format.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        filename_or_buf (str | TextIO): filename or StringIO buffer.
        geometry_column (str, optional): Column to use as a geometry. Defaults to "geometry".
        crs (int | str, optional): Coordinate system EPSG code or full geometry description name given as string.
        Defaults to 4326.
    """
    logger.debug("Saving geojson" + (f' to "{filename_or_buf}"' if isinstance(filename_or_buf, str) else ""))
    serializable_types = ["object", "int64", "float64", "bool"]

    if geometry_column not in dataframe.columns:
        logger.error(f'Geometry column "{geometry_column}" is not present, aborting')
        return

    geometry_series = dataframe[geometry_column]
    dataframe = dataframe.drop(geometry_column, axis=1)

    for col in set(dataframe.columns):
        if isinstance(dataframe[col], pd.DataFrame):
            logger.warning(f'Table contains multiple columns having with the same name: "{col}", renaming')
            overlapping_columns_number_range = iter(range(dataframe.shape[1] + 1))
            dataframe = dataframe.rename(
                lambda name, col=col, rng=overlapping_columns_number_range: name
                if name != col
                else f"{col}_{next(rng)}",
                axis=1,
            )
            for col_idx in range(next(overlapping_columns_number_range)):
                if dataframe[col_name:=f"{col}_{col_idx}"].dtypes not in serializable_types:
                    logger.warning(f'Dropping non-serializable "{col_name}" column')
                    dataframe = dataframe.drop(col_name, axis=1)
        else:
            if dataframe[col].dtypes not in serializable_types:
                logger.warning(f'Dropping non-serializable "{col}" column')
                dataframe = dataframe.drop(col, axis=1)

    if dataframe.shape[0] > 0:
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

    geojson = {
        "type": "FeatureCollection",
        "crs": {
            "type": "name",
            "properties": {
                "name": f"urn:ogc:def:crs:EPSG::{crs}" if isinstance(crs, int) else crs,
            },
        },
        "features": [
            {
                "type": "Feature",
                "properties": dict(row),
                "geometry": geometry,
            }
            for (_, row), geometry in zip(dataframe.iterrows(), geometry_series)
        ],
    }
    if isinstance(filename_or_buf, str):
        geojson["name"] = filename_or_buf
        with open(filename_or_buf, "w", encoding="utf-8") as file:
            json.dump(geojson, file, ensure_ascii=False, cls=NpEncoder)
    else:
        json.dump(geojson, filename_or_buf, ensure_ascii=False, cls=NpEncoder)

    logger.debug("Saved")
