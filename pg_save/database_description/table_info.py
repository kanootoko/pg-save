"""Table information functions are defined here."""

from typing import Union

import pandas as pd
import psycopg2

from pg_save.exceptions.table_not_fonud import NoTableOrMatViewFoundError
from pg_save.utils.print import print_df


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
        " FROM (SELECT a.attname AS column, pg_catalog.format_type(a.atttypid, a.atttypmod) AS datatype"
        "   FROM pg_catalog.pg_attribute a"
        "   WHERE"
        "       a.attnum > 0"
        "       AND NOT a.attisdropped"
        "       AND a.attrelid = ("
        "           SELECT c.oid FROM pg_catalog.pg_class c"
        "               LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace"
        "           WHERE n.nspname = %(schema)s AND c.relname = %(table_name)s"
        "       )"
        " ) as types"
        "   JOIN information_schema.columns c"
        "       ON types.column = c.column_name AND table_schema = %(schema)s AND table_name = %(table_name)s",
        {"schema": schema, "table_name": table},
    )
    table_df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    if not table_df.empty:
        return table_df
    cur.execute(
        "SELECT"
        "   a.attname AS column,"
        "   pg_catalog.format_type(a.atttypid, a.atttypmod) AS datatype,"
        "   not a.attnotnull AS is_nullable"
        " FROM pg_attribute a"
        "   JOIN pg_class t on a.attrelid = t.oid"
        "   JOIN pg_namespace s on t.relnamespace = s.oid"
        " WHERE a.attnum > 0"
        "   AND NOT a.attisdropped"
        "   AND t.relname = %(table_name)s"
        "   AND s.nspname = %(schema)s"
        "ORDER BY a.attnum",
        {"schema": schema, "table_name": table},
    )
    table_df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])
    if not table_df.empty:
        return table_df
    raise NoTableOrMatViewFoundError(table)


def describe_table(conn_or_cur: Union["psycopg2.connection", "psycopg2.cursor"], table: str) -> None:
    """Print table description to terminal.

    Args:
        conn_or_cur (Union[psycopg2.connection, psycopg2.cursor]): psycopg2 connection or cursor to access the database.
        table (str): table name (or schema.table) to get description for.
    """
    if isinstance(conn_or_cur, psycopg2.extensions.connection):
        with conn_or_cur, conn_or_cur.cursor() as cur:
            description = get_table_description(cur, table)
    else:
        description = get_table_description(conn_or_cur, table)
    print_df(description, limit_rows=False)
