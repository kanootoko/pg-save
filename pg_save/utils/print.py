"""Dataframe printing method is defined here."""

from __future__ import annotations

import os

import pandas as pd


def print_df(table_df: pd.DataFrame, limit_rows: bool = True, replace_na_str: str | None = "\\NULL") -> None:
    """Print pandas.DataFrame limiting rows and columns to current console size."""
    try:
        term_size = os.get_terminal_size()
        lines = term_size.lines
        columns = term_size.columns
    except Exception:  # pylint: disable=broad-except
        lines = 40
        columns = 80
    with pd.option_context(
        "display.min_rows",
        (lines - 6 if limit_rows else None),
        "display.max_rows",
        (lines - 5 if limit_rows else None),
        "display.width",
        columns,
        "display.max_colwidth",
        columns // table_df.shape[1],
    ):
        if replace_na_str is not None:
            print(table_df.infer_objects().fillna(replace_na_str))
        else:
            print(table_df)
