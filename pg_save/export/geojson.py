"""Logic of exporting pandas DataFrame to GeoJSON is defined here."""

from __future__ import annotations

import json
from typing import TextIO

import pandas as pd
from loguru import logger

from pg_save.export.default_crs import DEFAULT_CRS
from pg_save.utils import NpEncoder
from pg_save.utils.pd import beautify_dataframe


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
        logger.error('Geometry column "{}" is not present, aborting', geometry_column)
        return

    geometry_series = dataframe[geometry_column]
    dataframe = dataframe.drop(geometry_column, axis=1).copy()
    if not isinstance(dataframe.index, pd.RangeIndex) or not all(dataframe.index == pd.RangeIndex(dataframe.shape[0])):
        dataframe = dataframe.reset_index()
    dataframe = beautify_dataframe(dataframe)

    for col in set(dataframe.columns):
        if dataframe[col].dtypes not in serializable_types:
            logger.warning(f'Dropping non-serializable "{col}" column')
            dataframe = dataframe.drop(col, axis=1)

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
