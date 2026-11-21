"""Entry point so the app starts without relying on PYTHONPATH.

Works under `python -I` (isolated mode ignores PYTHONPATH): we insert the
project root on sys.path ourselves before importing the package.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.server import main  # noqa: E402

if __name__ == "__main__":
    main()
