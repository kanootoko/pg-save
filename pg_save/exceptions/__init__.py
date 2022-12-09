class UnsafeExpressionException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(args, **kwargs)


__all__ = [
    "UnsafeExpressionException",
]
