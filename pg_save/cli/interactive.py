"""Interactive executable part of pg-save utility is defined here"""

import traceback

import click
from loguru import logger
from psycopg2 import errors as pg_errors

from pg_save import database_description
from pg_save.dtos import DatabaseConfigDto
from pg_save.exceptions import UnsafeExpressionError
from pg_save.utils import interactive as interactive_utils

from .group import main, pass_db_config


@main.command("interactive")
@pass_db_config
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
    "--execute_as_is",
    "-r",
    is_flag=True,
    help="Do not apply automatic ST_AsGeoJSON() call to geometry columns",
)
def interactive_mode(  # pylint: disable=too-many-branches,too-many-statements
    db_config: DatabaseConfigDto,
    geometry_column: str,
    use_centroids: bool,
    execute_as_is: bool,
) -> None:
    """Interactive mode helper. Parses commands and returns results without closing the connection to the database.

    Args:
        conn (psycopg2.connection): psycopg2 connection to the database.
        geometry_column (str): geometry column to use when exporting GeoJSON.
        use_centroids (bool): indicates whether geometry centroids will be used intead of a full geometry
        when exporting to GeoJSON.
        execute_as_is (bool): indicates whether geometry columns will not be cast by ST_AsGeoJSON.
    """

    help_str = (
        "Commands available:"
        "\tq, \\q, quit, exit - quit application\n"
        "\t<query/filename> [> filename] - execute one-lined select query (and save the result to file if given)\n"
        '\t"<query/filename>" [> filename] - execute select query (and save the result to file if given)\n'
        "\t\\s <table_name> [> filename] - select * from table name (and save the result to file if given)\n"
        "\t\\dt [schema] - list tables in the given schema, or in all schemas if not given\n"
        "\t\\d [schema.]<table> - get table description\n"
        "\t\\geometry_column, \\g - change geometry column [current: {geometry_column}]\n"
        "\t\\use_centroids, \\c - switch centroids usage on selecting tables [current {use_centroids}]\n"
        "\t\\execute_as_is, \\r - switch raw execution trigger [current {execute_as_is}]"
    )
    print(
        "You are in interactive mode.",
        help_str.format(
            geometry_column=geometry_column,
            use_centroids=use_centroids,
            execute_as_is=execute_as_is,
        ),
        "\thelp - show this message",
        sep="\n",
    )
    interrupt_count = 0
    conn = db_config.get_connection()
    while True:
        try:
            command = input(">> ")
            if command in ("q", "\\q", "quit", "exit"):
                break
            if command.startswith("\\dt"):
                schema = command.split()[1] if " " in command else None
                database_description.list_tables(conn, schema)
            elif command.startswith("\\d"):
                if " " not in command:
                    print("You must use \\d with table name after it, aborting")
                    continue
                database_description.describe_table(conn, command.split()[1])
            elif command.startswith("\\g"):
                if " " not in command:
                    print("You must use \\g with table name after it, aborting")
                    continue
                geometry_column = command.split()[1]
                print(f'Switched geometry column to "{geometry_column}"')
            elif command in ("\\use_centroids", "\\c"):
                use_centroids = not use_centroids
                print(f"Centroid usage is swithced to: {use_centroids}")
            elif command in ("\\execute_as_is", "\\r"):
                execute_as_is = not execute_as_is
                print(f"Executing raw statements is changed to: {execute_as_is}")
            elif command.startswith("\\s"):
                interactive_utils.select_table(command, conn)
            elif command[0] == '"':
                interactive_utils.quoted_command(command, conn, geometry_column, execute_as_is)
            elif command == "help":
                print(
                    help_str.format(
                        geometry_column=geometry_column, use_centroids=use_centroids, execute_as_is=execute_as_is
                    )
                )
            else:
                interactive_utils.command_with_save(command, conn, geometry_column, execute_as_is)
            interrupt_count = 0

        except KeyboardInterrupt:
            interrupt_count += 1
            if interrupt_count > 1:
                print("Second Ctrl+C, exiting")
                break
            print("Ctrl+C hit, interrupting. Use it again or type 'exit' to exit interactive mode")
        except pg_errors.UndefinedTable as exc:
            print(f"Table is not found: {exc.pgerror}")
        except pg_errors.UndefinedColumn as exc:
            print(f"Column is not found: {exc.pgerror}")
        except (pg_errors.UndefinedFunction, pg_errors.UndefinedParameter) as exc:
            print(f"Using undefined function: {exc.pgerror}")
        except pg_errors.SyntaxError as exc:
            print(f"Syntax error: {exc.pgerror}")
        except UnsafeExpressionError as exc:
            print("This utility is not meant to update data. To do so use psql for example. Aborting")
            print(f"Exact exception: {exc}")
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Exception occured: {exc!r}")
            logger.debug("{}", traceback.format_exc())
