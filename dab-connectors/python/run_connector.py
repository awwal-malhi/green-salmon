"""Databricks job entry point (spark_python_task). See connector_framework.launcher for arguments."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from connector_framework.launcher import main  # noqa: E402

if __name__ == "__main__":
    main()
