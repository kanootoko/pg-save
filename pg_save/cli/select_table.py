"""Select table command is defined here."""

import sys

import click
from psycopg2 import errors as pg_errors

import pg_save.export as export_df
import pg_save.querying as query_db
from pg_save.dtos.database import DatabaseConfigDto
from pg_save.utils.pd import beautify_dataframe
from pg_save.utils.print import print_df

from .group import main, pass_db_config


@main.command("select-table")
@pass_db_config
@click.argument("query", type=str, metavar="select query/filename", required=False)
@click.option(
    "--geometry_column",
    "-g",
    type=str,
    metavar="geometry",
    default="geometry",
    help="Set column name to use as geometry",
)
@click.option(
    "--use_centroids",
    "-c",
    is_flag=True,
    help="Apply ST_Centroid() to the geometry column on select",
)
@click.option(
    "--output_filename",
    "-o",
    type=str,
    metavar="path/to/file.[csv|xlsx|geojson|json]",
    default=None,
    help="Path of the file to save results (.csv, .xlsx, .geojson, .json extensions)",
)
def select_table(  # pylint: disable=too-many-branches,too-many-statements
    db_config: DatabaseConfigDto,
    geometry_column: str,
    use_centroids: bool,
    output_filename: str | None,
    query: str | None,
) -> None:
    """Select all columns and rows of a given table"""
    conn = db_config.get_connection()
    try:
        table_data, crs_dict = query_db.get_table(conn, query, use_centroids)
    except pg_errors.UndefinedTable as exc:
        print(f"Table is not found: {exc.pgerror}")
        sys.exit(1)

    table_data = beautify_dataframe(table_data)

    print_df(table_data)

    if output_filename is not None:
        if table_data.shape[0] == 0:
            print("Warning: select results are empty!")
        export_df.to_file(table_data, output_filename, crs_dict, geometry_column)
