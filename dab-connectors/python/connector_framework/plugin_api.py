"""Interface for Python plugins. Mirrors com.greensalmon.connectors.api.SourcePlugin."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession

CORRUPT_RECORD_COLUMN = "_corrupt_record"


class SourcePlugin(ABC):
    """A plugin turns a source into a DataFrame. Validation, logging, errors and the write are the core's job.

    Rows that cannot be parsed should be returned in a string column named ``_corrupt_record``
    rather than dropped; the core quarantines them.
    """

    #: Plugin name, e.g. ``pipe_delimited``. Must match the name the job asks for.
    name: str = ""
    #: Plugin version, e.g. ``1.0.0``. Must match the version the job asks for.
    version: str = ""

    @abstractmethod
    def read(self, spark: SparkSession, options: Mapping[str, Any]) -> DataFrame:
        """Return the source data. ``options`` is the ``source`` section of the run config."""
