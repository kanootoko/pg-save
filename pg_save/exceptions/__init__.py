"""
Exceptions raised by the module are defined here.
"""


class UnsafeExpressionException(RuntimeError):
    """
    Raised when query contains suspicious commands (e.g. insert, update, delete, ...)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(args, **kwargs)


__all__ = [
    "UnsafeExpressionException",
]
