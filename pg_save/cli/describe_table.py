"""List tables command is defined here."""

import sys

import click

from pg_save import database_description
from pg_save.dtos.database import DatabaseConfigDto
from pg_save.exceptions.table_not_fonud import NoTableOrMatViewFoundError

from .group import main, pass_db_config


@main.command("describe-table")
@pass_db_config
@click.argument("table_name")
def describe_table(db_config: DatabaseConfigDto, table_name: str):
    """Describe database table columns.

    Table name can be given as SCHEMA.TABLE_NAME or just TABLE_NAME which implies that its schema is "public"."""
    conn = db_config.get_connection()
    try:
        database_description.describe_table(conn, table_name)
    except NoTableOrMatViewFoundError:
        print(f"No table or view with name '{table_name}' is found")
        sys.exit(1)
    finally:
        conn.close()
