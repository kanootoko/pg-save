"""NoTableOrMatViewFoundError is defined here."""


class NoTableOrMatViewFoundError(RuntimeError):
    """Raised on an attempt to get information of a table, but no table with given name is found"""

    def __init__(self, table_name):
        self.table_name = table_name
