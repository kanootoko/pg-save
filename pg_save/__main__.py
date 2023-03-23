"""
pg_save is an utility which helps to export PostgreSQL tables data, including geo-spatial ones,
to the following formats:
    json, geojson, csv, xlsx
"""
import os
import sys
import traceback

import click
import psycopg2
from loguru import logger
from psycopg2 import errors as pg_errors

import pg_save.export as export_df
import pg_save.query as query_db
from pg_save import database_description
from pg_save.exceptions import UnsafeExpressionException
from pg_save.utils import interactive as interactive_utils
from pg_save.utils import read_envfile


def interactive_mode(  # pylint: disable=too-many-branches,too-many-statements
    conn: "psycopg2.connection",
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
    logger.debug("Entering interactive mode")
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
                print(help_str.format(geometry_column=geometry_column, use_centroids=use_centroids))
            else:
                interactive_utils.command_with_save(command, conn, geometry_column, execute_as_is)
            interrupt_count = 0

        except KeyboardInterrupt:
            interrupt_count += 1
            if interrupt_count > 1:
                print("Second Ctrl+C, exiting")
                break
            print("Ctrl+C hit, interrupting. Use it again or type 'exit' to exit interactive mode")
        except pg_errors.UndefinedTable as exc:  # pylint: disable=no-member
            print(f"Table is not found: {exc.pgerror}")
        except pg_errors.UndefinedColumn as exc:  # pylint: disable=no-member
            print(f"Column is not found: {exc.pgerror}")
        except (pg_errors.UndefinedFunction, pg_errors.UndefinedParameter) as exc:  # pylint: disable=no-member
            print(f"Using undefined function: {exc.pgerror}")
        except pg_errors.SyntaxError as exc:  # pylint: disable=no-member
            print(f"Syntax error: {exc.pgerror}")
        except UnsafeExpressionException as exc:
            print("This utility is not ment to update data, use other methods, aborting")
            print(f"Exact exception: {exc}")
        except RuntimeError as exc:
            print(f"Exception occured: {exc}")
            logger.debug(traceback.format_exc())


@click.command()
@click.option(
    "--db_addr",
    "-H",
    envvar="DB_ADDR",
    type=str,
    metavar="localhost",
    default="localhost",
    help="Database host addres",
)
@click.option(
    "--db_port",
    "-P",
    envvar="DB_PORT",
    type=int,
    metavar="5423",
    default=5432,
    help="Database host port",
)
@click.option(
    "--db_name",
    "-D",
    envvar="DB_NAME",
    type=str,
    metavar="postgres",
    default="postgres",
    help="Databse name",
)
@click.option(
    "--db_user",
    "-U",
    envvar="DB_USER",
    type=str,
    metavar="postgres",
    default="postgres",
    help="Database user",
)
@click.option(
    "--db_pass",
    "-W",
    envvar="DB_PASS",
    type=str,
    metavar="postgres",
    default="postgres",
    help="Database user password",
)
@click.option(
    "--geometry_column",
    "-g",
    type=str,
    metavar="geometry",
    default="geometry",
    help="Set column name to use as geometry",
)
@click.option("--use_centroids", "-c", is_flag=True, help="Load geometry columns as centroids")
@click.option("--list_tables", "-l", is_flag=True, help="List tables in database and quit")
@click.option(
    "--describe_table",
    "-d",
    type=str,
    metavar="table_name",
    default=None,
    help="Describe given table and quit",
)
@click.option("--interactive", "-i", is_flag=True, help="Launch in interactive mode")
@click.option(
    "--verbose_level",
    "-v",
    envvar="VERBOSE_LEVEL",
    type=click.Choice(["ERROR", "WARNING", "INFO", "DEBUG", "TRACE"], False),
    default="WARNING",
    help="Verbose level for the logging",
)
@click.option(
    "--execute_as_is",
    "-r",
    is_flag=True,
    help="Do not apply automatic ST_AsGeoJSON() to geometry columns",
)
@click.option(
    "--filename",
    "-f",
    type=str,
    metavar="path/to/file.[csv|xlsx|geojson|json]",
    default=None,
    help="Path of the file to save results (.csv, .xlsx, .geojson, .json extensions)",
)
@click.argument("query", type=str, metavar="query/select", required=False)
def main(  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
    db_addr: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_pass: str,
    geometry_column: str,
    use_centroids: bool,
    list_tables: bool,
    describe_table: str | None,
    interactive: bool,
    verbose_level: str,
    execute_as_is: bool,
    filename: str | None,
    query: str | None,
) -> None:
    """
    Execute query or select full table by name

    QUERY can be a filename with a text query, table name (optionally including schema) or a normal text select-query
    """
    logger.remove()
    logger.add(sys.stderr, level=verbose_level)

    if geometry_column is not None and geometry_column != "geometry" and filename is None:
        logger.warning("Geometry column is set, but saving to file is not configured")

    logger.info(f"Connecting to postgresql://{db_user}@{db_addr}:{db_port}/{db_name}")

    try:
        with psycopg2.connect(
            host=db_addr,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_pass,
            connect_timeout=10,
            application_name="IDU - DataFrame Saver App",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                assert cur.fetchone()[0] == 1, "Error on database connection"  # type: ignore
    except psycopg2.OperationalError as exc:
        logger.error(f"Error on database connection: {exc}")
        sys.exit(1)

    if query is not None and os.path.isfile(query):
        logger.info("query is treated as filename, reading query from file")
        try:
            with open(query, "r", encoding="utf-8") as file:
                query = file.read()
        except UnicodeDecodeError as exc:
            logger.error("Cannot read file in UTF-8 encoding: {!r}", exc)
            print(f"Cannot read file in UTF-8 encoding: {exc!r}")
        except RuntimeError as exc:
            logger.error("Exception on file read: {!r}", exc)
            print(f"Error on file read: {exc}")
            sys.exit(1)

    if interactive:
        if list_tables or describe_table is not None or filename is not None or query is not None:
            logger.warning(
                "Interactive mode is launching, but some extra parameters"
                " (--list_tables, --describe_table or a query) are given. Ignoring"
            )
        interactive_mode(conn, geometry_column, use_centroids, execute_as_is)
    elif list_tables:
        logger.debug("Listing tables in datbase")
        database_description.list_tables(conn)
    elif describe_table is not None:
        logger.debug(f'Describing "{describe_table}" table')
        database_description.describe_table(conn, describe_table)
    elif query is not None:
        try:
            if query.lower().startswith(("select", "with")):
                if use_centroids:
                    logger.warning("Option --use_centroids is ignored due to user query")
                table_data, crs_dict = query_db.select(conn, query, execute_as_is=execute_as_is)
            else:
                table_data, crs_dict = query_db.get_table(conn, query, use_centroids)
        except pg_errors.UndefinedTable as exc:  # pylint: disable=no-member
            print(f"Table is not found: {exc.pgerror}")
            sys.exit(1)
        except pg_errors.UndefinedColumn as exc:  # pylint: disable=no-member
            print(f"Column is not found: {exc.pgerror}")
            sys.exit(1)
        except (pg_errors.UndefinedFunction, pg_errors.UndefinedParameter) as exc:  # pylint: disable=no-member
            print(f"Using undefined function: {exc.pgerror}")
            sys.exit(1)
        except pg_errors.SyntaxError as exc:  # pylint: disable=no-member
            print(f"Syntax error: {exc.pgerror}")
            sys.exit(1)
        except UnsafeExpressionException:
            print("This utility is not ment to update data, use other methods, aborting")
            sys.exit(1)
        except RuntimeError as exc:
            print(f"Exception occured: {exc!r}")
            logger.debug(traceback.format_exc())
            sys.exit(1)

        print(table_data)

        if filename is not None:
            if table_data.shape[0] == 0:
                print("Warning: select results are empty!")
            export_df.to_file(table_data, filename, crs_dict, geometry_column)
    else:
        print("Error: no query, -l or -d is given, nothing to be done")
        sys.exit(1)


if __name__ == "__main__":
    read_envfile(os.environ.get("ENVFILE", ".env"))
    main()  # pylint: disable=no-value-for-parameter
