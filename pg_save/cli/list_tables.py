"""List tables command is defined here."""

import click

from pg_save import database_description
from pg_save.dtos.database import DatabaseConfigDto

from .group import main, pass_db_config


@main.command("list-tables")
@pass_db_config
@click.argument("schema", required=False, default=None)
def list_tables(db_config: DatabaseConfigDto, schema: str | None):
    """List tables in the given database.

    If schema is not set, tables from all schemas are listed
    """
    conn = db_config.get_connection()
    try:
        database_description.list_tables(conn, schema)
    finally:
        conn.close()
