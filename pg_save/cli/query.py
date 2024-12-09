"""Query command is defined here"""

import sys
import traceback
from pathlib import Path

import click
from loguru import logger
from psycopg2 import errors as pg_errors

import pg_save.export as export_df
import pg_save.querying as query_db
from pg_save import __version__ as version
from pg_save.dtos.database import DatabaseConfigDto
from pg_save.exceptions.unsafe_expression import UnsafeExpressionError
from pg_save.utils.pd import beautify_dataframe
from pg_save.utils.print import print_df

from .group import main, pass_db_config


@main.command("query")
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
    "--execute_as_is",
    "-r",
    is_flag=True,
    help="Do not apply automatic ST_AsGeoJSON() call to geometry columns",
)
@click.option(
    "--output_filename",
    "-o",
    type=str,
    metavar="path/to/file.[csv|xlsx|geojson|json]",
    default=None,
    help="Path of the file to save results (.csv, .xlsx, .geojson, .json extensions)",
)
def execute_query(  # pylint: disable=too-many-branches,too-many-statements
    db_config: DatabaseConfigDto,
    geometry_column: str,
    execute_as_is: bool,
    output_filename: str | None,
    query: str | None,
) -> None:
    """Execute query or select full table by name.

    Query can be a SELECT statement or a filename with a select statement.
    """

    if geometry_column is not None and geometry_column != "geometry" and output_filename is None:
        logger.warning("Geometry column is set, but saving to file is not configured")

    try:
        if query is not None and Path(query).exists():
            logger.info("Query is treated as filename, reading query from file")
            try:
                with open(query, "r", encoding="utf-8") as file:
                    query = file.read()
            except UnicodeDecodeError as exc:
                logger.error("Cannot read file in UTF-8 encoding: {!r}", exc)
                print(f"Cannot read file in UTF-8 encoding: {exc!r}")
            except Exception as exc:  # pylint: disable=broad-except
                logger.error("Exception on file read: {!r}", exc)
                print(f"Error on file read: {exc}")
                sys.exit(1)
    except OSError:
        pass

    conn = db_config.get_connection()
    try:
        table_data, crs_dict = query_db.select(conn, query, execute_as_is)
    except pg_errors.UndefinedTable as exc:
        print(f"Table is not found: {exc.pgerror}")
        sys.exit(1)
    except pg_errors.UndefinedColumn as exc:
        print(f"Column is not found: {exc.pgerror}")
        sys.exit(1)
    except (pg_errors.UndefinedFunction, pg_errors.UndefinedParameter) as exc:
        print(f"Using undefined function: {exc.pgerror}")
        sys.exit(1)
    except pg_errors.SyntaxError as exc:
        print(f"Syntax error: {exc.pgerror}")
        sys.exit(1)
    except UnsafeExpressionError:
        print("This utility is not meant to update data, use other methods, aborting")
        sys.exit(1)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Exception occured: {exc!r}")
        logger.debug("{}", traceback.format_exc())
        sys.exit(1)
    finally:
        conn.close()

    table_data = beautify_dataframe(table_data)

    print_df(table_data)

    if output_filename is not None:
        if table_data.shape[0] == 0:
            print("Warning: select results are empty!")
        export_df.to_file(table_data, output_filename, crs_dict, geometry_column)
