"""Select query execution method is defined here."""

from __future__ import annotations

import pandas as pd
import psycopg2
from loguru import logger

from pg_save.exceptions import UnsafeExpressionError


def select(
    conn: "psycopg2.connection", query: str, execute_as_is: bool = False
) -> tuple[pd.DataFrame, dict[str, int] | None]:
    """Wrapper for "`[query]`"

    Args:
        conn (psycopg2.connection): psycopg2 connection for the database.
        query (str): user defined query to execute
        execute_as_is (bool, optional): indicates whether geometry columns should not be casted to ST_AsGeoJSON.
        Defaults to False.

    Raises:
        UnsafeExpressionException: raised when query contains suspicious commands (e.g. insert, update, delete, ...)

    Returns:
        tuple[pd.DataFrame, dict[str, int] | None]: DataFrame of table data and mapping {column: crs}
        for `geometry` columns. If table does not have those, then null.
    """
    stop_phrases = {
        "update ",
        "drop ",
        "insert ",
        "create ",
        ";",
        "alter ",
        "deallocate ",
        "copy ",
        "move ",
        "import ",
        "reassign ",
        "grant ",
    }
    logger.trace("executig query: {}", query)

    query = query.strip().rstrip(";")
    for stop_phrase in stop_phrases:
        if stop_phrase in query.lower():
            logger.error(
                "query seems to do something more than select. Found stop phrase: {}\nquery: {}",
                stop_phrase,
                query,
            )
            raise UnsafeExpressionError(f"Query contains stop phrase: {stop_phrase}")

    crs_dict: dict[str, int] | None = None
    with conn, conn.cursor() as cur:
        cur.execute("SELECT oid FROM pg_type WHERE typname IN %s", (("geometry", "geography"),))
        geometry_types = set(r[0] for r in cur.fetchall())
        try:
            cur.execute(query)
        except Exception as exc:
            logger.error("Error on user SELECT query: '{}', error: {!r}", query, exc)
            raise

        dataframe = pd.DataFrame(cur.fetchall(), columns=[desc.name for desc in cur.description])

        if not execute_as_is and dataframe.shape[0] > 0:
            for desc in cur.description:
                if desc.type_code in geometry_types:
                    if crs_dict is None:
                        crs_dict = {}
                    cur.execute(
                        "SELECT ST_SRID(geom::geometry) FROM (VALUES (%s)) tmp(geom)",
                        (dataframe.iloc[0][desc.name],),
                    )
                    crs_dict[desc.name] = cur.fetchone()[0]

                    cur.execute(
                        "SELECT ST_AsGeoJSON(geom::geometry)::jsonb"
                        f" FROM (VALUES {', '.join(('(%s)',) * dataframe.shape[0])}) tmp(geom)",
                        list(dataframe[desc.name]),
                    )
                    dataframe[desc.name] = [r[0] for r in cur.fetchall()]

    return dataframe, crs_dict
