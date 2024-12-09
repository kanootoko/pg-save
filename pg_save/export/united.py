"""United export to a given format methods are defined here."""

from typing import BinaryIO, Literal, TextIO

import pandas as pd
from loguru import logger

from pg_save.export.csv import to_csv
from pg_save.export.default_crs import DEFAULT_CRS
from pg_save.export.excel import to_excel
from pg_save.export.geojson import to_geojson
from pg_save.export.json import to_json


def to_file(
    dataframe: pd.DataFrame,
    filename: str,
    crs: dict[str, str | int] | int | str | None = None,
    geometry_column: str | None = None,
):
    """Export pandas DataFrame to the file given by filename.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        filename (str): Filename to export DataFrame to.
        crs (dict[str, int] | int | None, optional): Coordinate system given by name (string), EPSG code (int) or
        by mapping {column: crs}. Used only in export to GeoJSON. Defaults to None.
        geometry_column (str | None, optional): Column with geometry. Used only in export to GeoJSON. Defaults to None.
    """
    if "." not in filename:
        logger.warning("File does not have extension, using csv")
        filename += ".csv"
    file_format = filename.split(".")[-1].lower()
    if file_format.lower() not in ("csv", "xlsx", "json", "geojson"):
        logger.error('File has wrong extension ("{}"), switching to .csv', file_format)
        filename += ".csv"
        file_format = "csv"
    logger.info(f"Saving file in {file_format} format")
    if file_format == "csv":
        to_csv(dataframe, filename)
    elif file_format == "xlsx":
        to_excel(dataframe, filename)
    elif file_format == "geojson":
        if crs is None:
            logger.warning("No CRS is given, using {}", DEFAULT_CRS)
            crs = DEFAULT_CRS
        elif isinstance(crs, dict):
            if geometry_column in crs:
                crs = crs[geometry_column]
            else:
                logger.warning(
                    "crs given as a dict, but geometry column ({}) is not in its keys ({}). Using {}",
                    geometry_column,
                    ", ".join(crs.keys()),
                    DEFAULT_CRS,
                )
                crs = DEFAULT_CRS
        if geometry_column is None:
            logger.error('Geometry column is not set, but is required. Falling back to "geometry"')
            geometry_column = "geometry"
        to_geojson(dataframe, filename, geometry_column, crs)
    elif file_format == "json":
        to_json(dataframe, filename)


def to_buffer(
    dataframe: pd.DataFrame,
    buffer: TextIO | BinaryIO,
    file_format: Literal["csv", "xlsx", "geojson", "json"],
    crs: dict[str, str | int] | int | str | None = None,
    geometry_column: str | None = None,
) -> None:
    """Export pandas DataFrame to buffer.

    Args:
        dataframe (pd.DataFrame): DataFrame to export.
        buffer (TextIO | BinaryIO): StringIO (for csv, json, geojson) or BytesIO (for xlsx) to export DataFrame to.
        format (Literal["csv", "xlsx", "geojson", "json"]): file format to export to buffer.
        crs (int | str, optional): Coordinate system given by name (string), EPSG code (int) or
        by mapping {column: crs}. Used only in export to GeoJSON. Defaults to None.
        geometry_column (str): Column with geometry. Used only in export to GeoJSON. Defaults to None.
    """
    file_format = file_format.lower()
    if file_format not in ("csv", "xlsx", "json", "geojson"):
        logger.error('Format is not supported ("{}"), switching to csv', file_format)
        file_format = "csv"
    logger.info(f"Saving file in {file_format} format")
    if file_format == "csv":
        to_csv(dataframe, buffer)
    elif file_format == "xlsx":
        to_excel(dataframe, buffer)
    elif file_format == "geojson":
        if crs is None:
            logger.warning("No CRS is given, using {}", DEFAULT_CRS)
            crs = DEFAULT_CRS
        elif isinstance(crs, dict):
            if geometry_column in crs:
                crs = crs[geometry_column]
            else:
                logger.warning(
                    "crs given as a dict, but geometry column ({}) is not in its keys ({}). Using {}",
                    geometry_column,
                    ", ".join(crs.keys()),
                    DEFAULT_CRS,
                )
                crs = DEFAULT_CRS
        if geometry_column is None:
            logger.error('Geometry column is not set, but is required. Falling back to "geometry"')
            geometry_column = "geometry"
        to_geojson(dataframe, buffer, geometry_column, crs)
    elif file_format == "json":
        to_json(dataframe, buffer)
