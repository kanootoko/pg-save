import os

import psycopg2
from loguru import logger

import pg_save.export as export_df
import pg_save.query as query_db


def select_table(command, conn: "psycopg2.connection") -> None:
    if " " not in command:
        print("You must use \\s with table name after it, aborting")
        return
    filename = None
    table_end = len(command)
    if ">" in command:
        filename = command[command.rfind(">") + 1 :].strip().strip("'\"")
        table_end = command.rfind(">")
        logger.debug(f'Saving table select to file "{filename}"')

    table_name = command[2:table_end].strip()
    logger.debug(f"Selecting table {table_name}")
    table_data, _ = query_db.get_table(conn, table_name)

    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename)


def quoted_command(command: str, conn: "psycopg2.connection", geometry_column: str, execute_as_is: bool) -> None:
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
        logger.debug(f'Saving query to file "{filename}"')
    query = command[1:query_end].strip()
    if os.path.isfile(query):
        logger.info("query_db is treated as filename, reading query from file")
        try:
            with open(query, "r", encoding="utf-8") as file:
                query = file.read()
        except Exception as ex:
            logger.error(f"Exception on file read: {ex}")
    logger.debug(f"Running query: {query}")

    table_data, crs_dict = query_db.select(conn, query, execute_as_is)
    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename, crs_dict, geometry_column)

def command_with_save(command: str, conn: "psycopg2.connection", geometry_column: str, execute_as_is: bool) -> None:
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
        except Exception as ex:
            logger.error(f"Exception on file read: {ex}")

    logger.debug(f"running query (no options left): {query}")

    table_data, crs_dict = query_db.select(conn, command, execute_as_is)
    print(table_data)

    if filename is not None:
        export_df.to_file(table_data, filename, crs_dict, geometry_column)

__all__ = [
    "select_table",
    "quoted_command",
    "command_with_save",
]
