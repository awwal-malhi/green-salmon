package com.greensalmon.plugins.csv;

import com.greensalmon.connectors.api.PluginConfig;
import com.greensalmon.connectors.api.SourcePlugin;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 * CSV plugin, version 2. Same class name as v1 on purpose: the core loads each version in its own
 * class loader. Changes from v1:
 * <ul>
 *   <li>leading/trailing whitespace around values is trimmed (option {@code trim}, default true);</li>
 *   <li>lines starting with the {@code comment} character (default {@code #}) are skipped;</li>
 *   <li>a {@code _source_file} column records which file each row came from.</li>
 * </ul>
 * Options as v1 plus {@code trim} and {@code comment}.
 */
public final class CsvSourcePlugin implements SourcePlugin {

    public static final String SOURCE_FILE_COLUMN = "_source_file";

    @Override
    public String name() {
        return "csv";
    }

    @Override
    public String version() {
        return "2.0.0";
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
                .csv(path)
                .withColumn(SOURCE_FILE_COLUMN, functions.col("_metadata.file_path"));
    }

    private static DataFrameReader baseReader(SparkSession spark, PluginConfig config, boolean header) {
        boolean trim = config.getBoolean("trim", true);
        return spark.read()
                .option("header", String.valueOf(header))
                .option("encoding", config.getString("encoding", "UTF-8"))
                .option("comment", config.getString("comment", "#"))
                .option("ignoreLeadingWhiteSpace", String.valueOf(trim))
                .option("ignoreTrailingWhiteSpace", String.valueOf(trim));
    }
}
