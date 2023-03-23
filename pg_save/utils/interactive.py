"""
Interactive methods of the project are defined here.
"""
import os

import psycopg2  # pylint: disable=unused-import
from loguru import logger

import pg_save.export as export_df
import pg_save.query as query_db


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
        logger.debug(f'saving table select to file "{filename}"')

    table_name = command[2:table_end].strip()
    logger.debug(f"selecting table {table_name}")
    table_data, _ = query_db.get_table(conn, table_name)

    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename)


def quoted_command(command: str, conn: "psycopg2.connection", geometry_column: str, execute_as_is: bool) -> None:
    """Quoted command helper, launches when command starts with a double quote.

    Args:
        command (str): command to execute.
        conn (psycopg2.connection): psycopg2 connection to the database.
        geometry_column (str): geometry column for GeoJSON export.
        execute_as_is (bool): indicates whether geometry columns will not be casted by ST_AsGeoJSON.
    """
    if command.find('"', 1) == -1 or command.count('"') - command.count('\\"') == 0:
        while True:
            try:
                line = input('>>>"')
                command += f" {line}"
                if line.find('"', 1) != -1 or line.count('"') - line.count('\\"') != 0:
                    break
            except KeyboardInterrupt:
                print("Ctrl+C hit, aborting query")
                command = ""
                break
        if command == "":
            return
    filename = None
    query_end = command.rfind('"')

    if ">" in command:
        filename = command[command.rfind(">") + 1 :].strip().strip("'\"")
        query_end = command.rfind('"', 2, command.rfind(">"))
        logger.debug(f'Saving query results to file "{filename}"')

    query = command[1:query_end].strip()
    if os.path.isfile(query):
        logger.info("query is treated as filename, reading query from file")
        try:
            with open(query, "r", encoding="utf-8") as file:
                query = file.read()
        except RuntimeError as ex:
            logger.error(f"Exception on file read: {ex}")

    logger.debug("executing query: {}", query)

    table_data, crs_dict = query_db.select(conn, query, execute_as_is)
    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename, crs_dict, geometry_column)


def command_with_save(command: str, conn: "psycopg2.connection", geometry_column: str, execute_as_is: bool) -> None:
    """Command with export to flie helper.

    Args:
        command (str): command to execute.
        conn (psycopg2.connection): psycopg2 connection to the database.
        geometry_column (str): geometry column to use with export to GeoJSON.
        execute_as_is (bool): indicates whether geometry columns will not be casted by ST_AsGeoJSON.
    """
    if ">" in command:
        query = command[: command.find(">")].strip()
        filename = command[command.find(">") + 1 :].strip().strip("'\"")
    else:
        query = command
        filename = None

    if os.path.isfile(query):
        logger.info("query is treated as filename, reading query from file")
        try:
            with open(query, "r", encoding="utf-8") as file:
                query = file.read()
        except RuntimeError as ex:
            logger.error(f"Exception on file read: {ex}")

    logger.debug("executing query (no options left): {}", query)

    table_data, crs_dict = query_db.select(conn, command, execute_as_is)
    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename, crs_dict, geometry_column)


__all__ = [
    "select_table",
    "quoted_command",
    "command_with_save",
]
