"""
Database description functions are defined here.
"""
import os
from typing import Union

import pandas as pd
import psycopg2


def get_table_description(cur: "psycopg2.cursor", table: str) -> pd.DataFrame:
    """Return table columns and types list as pandas DataFrame.

    Args:
        cur (psycopg2.cursor): psycopg2 cursor to access the database.
        table (str): table name (or schema.table) to get description for.

    Returns:
        pd.DataFrame: columns description containing columns: [column, datatype, is_nullable, column_default].
    """
    if "." in table:
        schema, table = table.split(".")
    else:
        schema = "public"
    cur.execute(
        "SELECT types.column, types.datatype, is_nullable, column_default AS default"
        "FROM (SELECT a.attname AS column, pg_catalog.format_type(a.atttypid, a.atttypmod) AS datatype"
        "    FROM pg_catalog.pg_attribute a"
        "    WHERE"
        "      a.attnum > 0"
        "      AND NOT a.attisdropped"
        "      AND a.attrelid = ("
        "          SELECT c.oid FROM pg_catalog.pg_class c"
        "              LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
        "          WHERE n.nspname = %(schema)s AND c.relname = %(table_name)s"
        "      )"
        " ) as types"
        "   JOIN information_schema.columns c"
        "       ON types.column = c.column_name AND table_schema = %(schema)s AND table_name = %(table_name)s",
        {"schema": schema, "table_name": table},
    )
    return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


def get_tables_list(cur: "psycopg2.cursor", schema: str | None = None) -> pd.DataFrame:
    """Return tables list as pandas DataFrame

    Args:
        cur (psycopg2.cursor): psycopg2 cursor to access the database.
        schema (str | None, optional): schema name to get tables for. Return tables for all schemas if empty.
        Defaults to None.

    Returns:
        pd.DataFrame: table list as DataFrame containing columns [schema, table].
    """
    cur.execute(
        "SELECT table_schema as schema, table_name as table"
        " FROM information_schema.tables"
        " WHERE table_name NOT LIKE 'pg_%%'"
        "   AND table_schema NOT IN ('pg_catalog', 'information_schema', 'topology')"
        + (" AND table_schema = %s" if schema is not None else "")
        + " ORDER BY table_schema, table_name",
        ((schema,) if schema is not None else None),
    )
    return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


def describe_table(conn_or_cur: Union["psycopg2.connection", "psycopg2.cursor"], table: str) -> None:
    """Print table description to terminal.

    Args:
        conn_or_cur (Union[psycopg2.connection, psycopg2.cursor]): psycopg2 connection or cursor to access the database.
        table (str): table name (or schema.table) to get description for.
    """
    if isinstance(conn_or_cur, psycopg2.extensions.connection):  # type: ignore
        with conn_or_cur, conn_or_cur.cursor() as cur:  # type: ignore
            description = get_table_description(cur, table)
    else:
        description = get_table_description(conn_or_cur, table)  # type: ignore
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        os.get_terminal_size().columns,
    ):  # more options can be specified also
        print(description.fillna(""))


def list_tables(
    conn_or_cur: Union["psycopg2.connection", "psycopg2.cursor"],
    schema: str | None = None,
) -> None:
    """Print tables list to terminal.

    Args:
        conn_or_cur (Union[psycopg2.connection, psycopg2.cursor]): psycopg2 connection or cursor to access the database.
        schema (str | None, optional): schema name to get tables for. Return tables for all schemas if empty.
        Defaults to None.
    """
    if isinstance(conn_or_cur, psycopg2.extensions.connection):  # type: ignore
        with conn_or_cur, conn_or_cur.cursor() as cur:  # type: ignore
            description = get_tables_list(cur, schema)
    else:
        description = get_tables_list(conn_or_cur, schema)  # type: ignore

    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        os.get_terminal_size().columns,
    ):  # more options can be specified also
        print(description.fillna(""))


__all__ = [
    "get_table_description",
    "get_tables_list",
    "describe_table",
    "list_tables",
]
