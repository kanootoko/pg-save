from typing import BinaryIO, TextIO

import pandas as pd
from loguru import logger

from pg_save.export.csv import to_csv
from pg_save.export.default_crs import default_crs
from pg_save.export.excel import to_excel
from pg_save.export.geojson import to_geojson
from pg_save.export.json import to_json


def to_file(
    df: pd.DataFrame, filename: str, crs: dict[str, int] | int | None = None, geometry_column: str | None = None
):
    if "." not in filename:
        logger.warning("File does not have extension, using csv")
        filename += ".csv"
    file_format = filename.split(".")[-1]
    if file_format not in ("csv", "xlsx", "json", "geojson"):
        logger.error(f'File has wrong extension ("{file_format}"), switching to .csv')
        filename += ".csv"
        file_format = "csv"
    logger.info(f"Saving file in {file_format} format")
    if file_format == "csv":
        to_csv(df, filename)
    elif file_format == "xlsx":
        to_excel(df, filename)
    elif file_format == "geojson":
        if crs is None:
            logger.warning("No CRS is given, using {}", default_crs)
            crs = 4326
        if isinstance(crs, dict):
            if geometry_column in crs:
                crs = crs[geometry_column]
            else:
                logger.warning(
                    "crs given as a dict, but geometry column ({}) is not in its keys ({}). Using {}",
                    geometry_column,
                    ", ".join(crs.keys()),
                    default_crs,
                )
        if geometry_column is None:
            logger.error('Geometry column is not set, but is required. Falling back to "geometry"')
            geometry_column = "geometry"
        to_geojson(df, filename, geometry_column, crs)  # type: ignore
    elif file_format == "json":
        to_json(df, filename)


def to_buffer(
    df: pd.DataFrame, buffer: TextIO | BinaryIO, format: str, geometry_column: str | None = None, crs: int = default_crs
) -> None:
    if format not in ("csv", "xlsx", "json", "geojson"):
        logger.error(f'Format is not supported ("{format}"), switching to csv')
        format = "csv"
    logger.info(f"Saving file in {format} format")
    if format == "csv":
        to_csv(df, buffer)  # type: ignore
    elif format == "xlsx":
        to_excel(df, buffer)  # type: ignore
    elif format == "geojson":
        if geometry_column is None:
            logger.error('Geometry column is not set, but is required. Falling back to "geometry"')
            geometry_column = "geometry"
        to_geojson(df, buffer, geometry_column, crs)  # type: ignore
    elif format == "json":
        to_json(df, buffer)  # type: ignore


__all__ = [
    "default_crs",
    "to_csv",
    "to_excel",
    "to_geojson",
    "to_json",
    "to_file",
]
