"""Tables listing functions are defined here."""

from __future__ import annotations

from typing import Union

import pandas as pd
import psycopg2

from pg_save.utils.print import print_df


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
    if isinstance(conn_or_cur, psycopg2.extensions.connection):
        with conn_or_cur, conn_or_cur.cursor() as cur:
            description = get_tables_list(cur, schema)
    else:
        description = get_tables_list(conn_or_cur, schema)

    print_df(description, limit_rows=False)
