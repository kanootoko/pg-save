"""Main group is defined here."""

import sys

import click
from loguru import logger

from pg_save import __version__ as version
from pg_save.dtos import DatabaseConfigDto


pass_db_config = click.make_pass_decorator(DatabaseConfigDto)


@click.group("pg-save")
@click.version_option(version)
@click.pass_context
@click.option(
    "--db_host",
    "-h",
    envvar="DB_HOST",
    type=str,
    metavar="localhost",
    default="localhost",
    help="Database host address",
)
@click.option(
    "--db_port",
    "-p",
    envvar="DB_PORT",
    type=int,
    metavar="5423",
    default=5432,
    help="Database host port",
)
@click.option(
    "--db_name",
    "-d",
    envvar="DB_NAME",
    type=str,
    metavar="postgres",
    default="postgres",
    help="Databse name",
)
@click.option(
    "--db_user",
    "-u",
    envvar="DB_USER",
    type=str,
    metavar="postgres",
    default="postgres",
    help="Database user name",
)
@click.option(
    "--db_pass",
    "-w",
    envvar="DB_PASS",
    type=str,
    metavar="postgres",
    default="postgres",
    help="Database user password",
)
@click.option(
    "--verbose_level",
    "-v",
    envvar="VERBOSE_LEVEL",
    type=click.Choice(["ERROR", "WARNING", "INFO", "DEBUG", "TRACE"], False),
    default="WARNING",
    help="Verbose level for the logging",
)
def main(  # pylint: disable=too-many-arguments
    ctx: click.Context,
    *,
    db_host: str,
    db_port: int,
    db_name: str,
    db_user: str,
    db_pass: str,
    verbose_level: str,
):
    """pg-save can be used to see database schema, select tables data and export it in various formats"""
    ctx.obj = DatabaseConfigDto(db_host, db_port, db_name, db_user, db_pass)

    logger.remove()
    logger.add(sys.stderr, level=verbose_level)
