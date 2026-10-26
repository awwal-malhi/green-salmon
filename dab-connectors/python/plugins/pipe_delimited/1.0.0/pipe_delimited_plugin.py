"""Pipe-delimited file plugin, version 1.0.0.

Options (the ``source`` section of the run config):
  path      required; file or directory
  columns   optional list; default: taken from the header row
  header    default true
  delimiter default "|"
  encoding  default UTF-8

All columns are read as strings; typing is the core's job. Rows with the wrong number of fields
come back in ``_corrupt_record`` so the core can quarantine them.
"""

from __future__ import annotations

from typing import Any, Mapping

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from connector_framework.plugin_api import CORRUPT_RECORD_COLUMN, SourcePlugin


class PipeDelimitedPlugin(SourcePlugin):
    name = "pipe_delimited"
    version = "1.0.0"

    def read(self, spark: SparkSession, options: Mapping[str, Any]) -> DataFrame:
        if not options.get("path"):
            raise ValueError("pipe_delimited plugin: 'path' is required")
        path = options["path"]
        header = str(options.get("header", True)).lower() == "true"

        def reader():
            return (
                spark.read.option("header", str(header).lower())
                .option("sep", options.get("delimiter", "|"))
                .option("encoding", options.get("encoding", "UTF-8"))
            )

        columns = list(options.get("columns") or [])
        if not columns:
            if not header:
                raise ValueError("pipe_delimited plugin: 'columns' is required when header is false")
            columns = reader().csv(path).columns

        schema = StructType(
            [StructField(c, StringType()) for c in columns] + [StructField(CORRUPT_RECORD_COLUMN, StringType())]
        )
        return (
            reader()
            .schema(schema)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
            .csv(path)
        )
