"""Path bootstrap for the sql_process example.

Sits at ``examples/sql_process/_bootstrap.py``. Adds the repo root
to sys.path so ``from src.mdde_lite ...`` works regardless of cwd.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))   # .../examples/sql_process
_EXAMPLES = os.path.dirname(_HERE)                   # .../examples
_REPO = os.path.dirname(_EXAMPLES)                   # mdde-demo repo root

if _REPO not in sys.path:
    sys.path.insert(0, _REPO)
