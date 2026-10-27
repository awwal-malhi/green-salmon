"""Databricks job entry point (spark_python_task). See connector_framework.launcher for arguments."""

import os
import sys

# spark_python_task runs this file with exec(compile(source, path, "exec")), which leaves
# __file__ undefined; the code object's filename is the path in both cases.
sys.path.insert(0, os.path.dirname(os.path.abspath(sys._getframe().f_code.co_filename)))

from connector_framework.launcher import main  # noqa: E402

if __name__ == "__main__":
    main()
