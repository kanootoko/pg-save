"""Query wrappers are defined here.

Raises:
    UnsafeExpressionException: raised when query contains suspicious commands (e.g. insert, update, delete, ...)
"""

from .query import select
from .table import get_table
