"""Select table method is defined here."""

from __future__ import annotations

import pandas as pd
import psycopg2
from loguru import logger


def get_table(
    conn: "psycopg2.connection", table: str, use_centroids: bool = False
) -> tuple[pd.DataFrame, dict[str, int] | None]:
    """Wrapper for "SELECT * FROM `[table]`"

    Args:
        conn (psycopg2.connection): psycopg2 connection for the database.
        table (str): database table name (or schema.table)
        use_centroids (bool, optional): indicates whether to use geometry centroids or full geometry.
        Used only with `geometry` fields. Defaults to False.

    Returns:
        tuple[pd.DataFrame, dict[str, int] | None]: DataFrame of table data and mapping {column: crs}
        for `geometry` columns. If table does not have those, then null.
    """
    crs_dict: dict[str, int] | None = None
    logger.trace("executing SELECT on table {}. use_centroids: {}", table, use_centroids)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT oid FROM pg_type WHERE typname IN %s", (("geometry", "geography"),))
        geometry_types = set(r[0] for r in cur.fetchall())

        cur.execute(f"SELECT * from {table} LIMIT 0")
        columns_to_select = []
        for desc in cur.description:
            if desc.type_code not in geometry_types:
                columns_to_select.append(desc.name)
            else:
                if use_centroids:
                    columns_to_select.append(f'ST_AsGeoJSON(ST_Centroid("{desc.name}"))::jsonb "{desc.name}"')
                else:
                    columns_to_select.append(f'ST_AsGeoJSON("{desc.name}")::jsonb "{desc.name}"')
                cur.execute(f"SELECT ST_SRID({desc.name}) FROM {table} LIMIT 1")
                res = cur.fetchone()
                if res is not None:
                    if crs_dict is None:
                        crs_dict = {}
                    crs_dict[desc.name] = res[0]

        cur.execute(f'SELECT {", ".join(columns_to_select)} FROM {table}')

        dataframe = pd.DataFrame(cur.fetchall(), columns=[desc.name for desc in cur.description])

    return dataframe, crs_dict
