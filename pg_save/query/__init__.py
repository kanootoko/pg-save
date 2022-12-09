import pandas as pd
import psycopg2
from loguru import logger

from pg_save.exceptions import UnsafeExpressionException


def get_table(
    conn: "psycopg2.connection", table: str, use_centroids: bool = False
) -> tuple[pd.DataFrame, dict[str, int] | None]:
    crs_dict: dict[str, int] | None = None
    logger.trace("executing SELECT on table {}. use_centroids: {}", table, use_centroids)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT oid FROM pg_type WHERE typname IN %s", (("geometry", "geography"),))
        geometry_types = set(r[0] for r in cur.fetchall())

        cur.execute(f"SELECT * from {table} LIMIT 0")
        columns_to_select = []
        for d in cur.description:
            if d.type_code not in geometry_types:
                columns_to_select.append(d.name)
            else:
                if use_centroids:
                    columns_to_select.append(f'ST_AsGeoJSON(ST_Centroid("{d.name}"))::jsonb "{d.name}"')
                else:
                    columns_to_select.append(f'ST_AsGeoJSON("{d.name}")::jsonb "{d.name}"')
                cur.execute(f"SELECT ST_SRID({d.name}) FROM {table} LIMIT 1")
                if (res := cur.fetchone()) is not None:
                    if crs_dict is None:
                        crs_dict = {}
                    crs_dict[d.name] = res[0]  # type: ignore

        cur.execute(f'SELECT {", ".join(columns_to_select)} FROM {table}')

        df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

    return df, crs_dict


def select(
    conn: "psycopg2.connection", query: str, execute_as_is: bool = False
) -> tuple[pd.DataFrame, dict[str, int] | None]:
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

    if any(stop_phrase in query.lower() for stop_phrase in stop_phrases):
        logger.error(
            "query seems to do something more than select."
            " Found one of stop-phrases: 'update', 'drop', 'insert', 'create', ';', ...\nquery: {}",
            query,
        )
        raise UnsafeExpressionException("Query seems to be unsafe")

    crs_dict: dict[str, int] | None = None
    with conn, conn.cursor() as cur:
        cur.execute("SELECT oid FROM pg_type WHERE typname IN %s", (("geometry", "geography"),))
        geometry_types = set(r[0] for r in cur.fetchall())
        try:
            cur.execute(query)
        except Exception as e:
            logger.error("Error on user SELECT query: '{}', error: {!r}", query, e)
            raise

        df = pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])

        if not execute_as_is and df.shape[0] > 0:
            for d in cur.description:
                if d.type_code in geometry_types:
                    if crs_dict is None:
                        crs_dict = {}
                    cur.execute("SELECT ST_SRID(geom::geometry) FROM (VALUES (%s)) tmp(geom)", (df.iloc[0][d.name],))
                    crs_dict[d.name] = cur.fetchone()[0]  # type: ignore

                    cur.execute(
                        f"SELECT ST_AsGeoJSON(geom::geometry)::jsonb FROM (VALUES {', '.join(('(%s)',) * df.shape[0])}) tmp(geom)",
                        list(df[d.name]),
                    )
                    df[d.name] = [r[0] for r in cur.fetchall()]

    return df, crs_dict


__all__ = [
    "get_table",
    "select",
]
