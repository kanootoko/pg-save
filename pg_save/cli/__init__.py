"""Click command-line executable part of pg-save utility is located here."""

from pg_save.utils import try_read_envfile


try_read_envfile()

from . import query  # pylint: disable=wrong-import-position; isort: skip
from . import select_table  # pylint: disable=wrong-import-position; isort: skip
from . import list_tables  # pylint: disable=wrong-import-position; isort: skip
from . import describe_table  # pylint: disable=wrong-import-position; isort: skip
from . import interactive  # pylint: disable=wrong-import-position; isort: skip
from .group import main  # pylint: disable=wrong-import-position; isort: skip
