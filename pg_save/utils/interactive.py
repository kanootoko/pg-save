"""Interactive methods of the project are defined here."""

import os
import sys

import psycopg2  # pylint: disable=unused-import
from loguru import logger

import pg_save.export as export_df
import pg_save.querying as query_db
from pg_save.utils.pd import beautify_dataframe
from pg_save.utils.print import print_df


def select_table(command: str, conn: "psycopg2.connection") -> None:
    """Select table helper

    Args:
        command (str): command to execute.
        conn (psycopg2.connection): psycopg2 connection to the database.
    """
    if " " not in command:
        print("You must use \\s with table name after it, aborting")
        return
    filename = None
    table_end = len(command)
    if ">" in command:
        filename = command[command.rfind(">") + 1 :].strip().strip("'\"")
        table_end = command.rfind(">")
        logger.debug('Saving table select to file "{}"', filename)

    table_name = command[2:table_end].strip()
    logger.debug("Selecting table {}", table_name)
    table_data, _ = query_db.get_table(conn, table_name)

    table_data = beautify_dataframe(table_data)

    print_df(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename)


def quoted_command(command: str, conn: "psycopg2.connection", geometry_column: str, execute_as_is: bool) -> None:
    """Quoted command helper. Launched when command starts with a double quote.

    Args:
        command (str): command to execute.
        conn (psycopg2.connection): psycopg2 connection to the database.
        geometry_column (str): geometry column for GeoJSON export.
        execute_as_is (bool): indicates whether geometry columns will not be casted by ST_AsGeoJSON.
    """
    if '"' not in command[1:] or command.count('"') - command.count('\\"') == 0:
        while True:
            try:
                line = input('>>>"')
                command += f" {line}"
                if line.count('"') - line.count('\\"') != 0:
                    break
            except KeyboardInterrupt:
                print("Ctrl+C hit, aborting multi-line query")
                command = ""
                break
        if command.strip() == "":
            return
    filename = None
    query_end = command.rfind('"')

    if line.rfind(">") > line.rfind('"'):
        filename = line[line.rfind(">") + 1 :].strip("'\" ").strip()
        query_end = command.rfind('"', 2, command.rfind(">"))
        logger.debug('Saving query results to file "{}"', filename)

    query = command[1:query_end].strip()
    query = query.replace('\\"', '"')
    if os.path.isfile(query):
        logger.info("Query is treated as filename, reading query from file")
        try:
            with open(query, "r", encoding="utf-8") as file:
                query = file.read()
        except RuntimeError as exc:
            logger.error("Exception on file read: {!r}", exc)

    logger.debug("Executing query: {}", query)

    table_data, crs_dict = query_db.select(conn, query, execute_as_is)
    table_data = beautify_dataframe(table_data)

    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename, crs_dict, geometry_column)


def command_with_save(command: str, conn: "psycopg2.connection", geometry_column: str, execute_as_is: bool) -> None:
    """Execute query with optional export to flie if '>' is in command.

    Args:
        command (str): command to execute.
        conn (psycopg2.connection): psycopg2 connection to the database.
        geometry_column (str): geometry column to use with export to GeoJSON.
        execute_as_is (bool): indicates whether geometry columns will not be casted by ST_AsGeoJSON.
    """
    if ">" in command:
        query = command[: command.rfind(">")].strip()
        filename = command[command.rfind(">") + 1 :].strip().strip("'\"")
    else:
        query = command
        filename = None

    if os.path.isfile(query):
        logger.info("Query is treated as filename, reading query from file")
        try:
            with open(query, "r", encoding="utf-8") as file:
                query = file.read()
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Exception on file read: {!r}", exc)
            sys.exit(1)

    logger.debug("Executing query (no options left): {}", query)

    table_data, crs_dict = query_db.select(conn, query, execute_as_is)

    table_data = beautify_dataframe(table_data)

    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename, crs_dict, geometry_column)
