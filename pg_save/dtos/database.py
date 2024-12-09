"""Database configuration DTO is defined here."""

from dataclasses import dataclass

import psycopg2
from loguru import logger

from pg_save import __version__ as version
from pg_save.exceptions import DbConnectionError


@dataclass
class DatabaseConfigDto:
    """Database configuration containing host address, port, database name, user and password.

    Connection can be established as postgresql://`user`:`password`@`host`:`port`/`database`"""

    host: str
    port: int
    database: str
    user: str
    password: str

    def format(self, include_password: bool = False) -> "str":
        """Format database connection string."""
        return (
            f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}"
            if not include_password
            else f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    def get_connection(self) -> "psycopg2.connection":
        """Get a psycopg2 connection with given parameters"""
        logger.debug("Connecting to {}", self.format())
        try:
            with psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=5,
                keepalives_interval=2,
                keepalives_count=2,
                application_name=f"pg_export v{version}",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    if cur.fetchone()[0] != 1:
                        raise DbConnectionError("Could not connect to the database")

        except psycopg2.OperationalError as exc:
            logger.error("Error on database connection: {!r}", exc)
            raise DbConnectionError() from exc

        return conn
