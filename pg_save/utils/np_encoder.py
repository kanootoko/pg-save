"""Numpy data json encoder is defnied here."""

import json
from typing import Any

import numpy as np


class NpEncoder(json.JSONEncoder):
    """JSON encoder to use with numpy/pandas datatypes."""

    def default(self, o: Any) -> Any:
        if isinstance(o, np.integer):
            return int(o)
        if isinstance(o, np.floating):
            return int(o) if o.is_integer() else float(o)
        if isinstance(o, np.ndarray):
            return o.tolist()
        return str(o)
