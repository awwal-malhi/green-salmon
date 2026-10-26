package com.greensalmon.plugins.csv;

import com.greensalmon.connectors.api.PluginConfig;
import com.greensalmon.connectors.api.SourcePlugin;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * CSV plugin, version 1. Reads comma-separated files with a header row. All columns are read as
 * strings (typing is the core's job); rows with the wrong number of fields come back in
 * {@code _corrupt_record} so the core can quarantine them.
 *
 * <p>Options: {@code path} (required), {@code columns} (list; default: taken from the header),
 * {@code header} (default true), {@code encoding} (default UTF-8).
 */
public final class CsvSourcePlugin implements SourcePlugin {

    @Override
    public String name() {
        return "csv";
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public Dataset<Row> read(SparkSession spark, PluginConfig config) {
        String path = config.getString("path");
        boolean header = config.getBoolean("header", true);
        List<String> columns = config.getStringList("columns");
        if (columns.isEmpty()) {
            if (!header) {
                throw new IllegalArgumentException("csv plugin: 'columns' is required when header is false");
            }
            columns = Arrays.asList(baseReader(spark, config, header).csv(path).columns());
        }
        StructType schema = new StructType();
        for (String column : columns) {
            schema = schema.add(column, DataTypes.StringType);
        }
        schema = schema.add(CORRUPT_RECORD_COLUMN, DataTypes.StringType);
        return baseReader(spark, config, header)
                .schema(schema)
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
                .csv(path);
    }

    private static DataFrameReader baseReader(SparkSession spark, PluginConfig config, boolean header) {
        return spark.read()
                .option("header", String.valueOf(header))
                .option("encoding", config.getString("encoding", "UTF-8"));
    }
}
