"""UnsafeExpression error is definde here."""


class UnsafeExpressionError(RuntimeError):
    """Raised when query contains suspicious commands (e.g. insert, update, delete, ...)"""
