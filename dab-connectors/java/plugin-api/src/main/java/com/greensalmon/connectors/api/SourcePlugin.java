package com.greensalmon.connectors.api;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * A connector plugin. Its only job is to turn a source into a DataFrame.
 * Validation, logging, error handling and the Delta write belong to the core.
 *
 * <p>Implementations are discovered with {@link java.util.ServiceLoader}: list the class in
 * {@code META-INF/services/com.greensalmon.connectors.api.SourcePlugin} inside the plugin JAR.
 * Implementations need a public no-argument constructor.
 *
 * <p>If the source can contain rows that cannot be parsed, return them in a string column named
 * {@link #CORRUPT_RECORD_COLUMN} instead of dropping them; the core quarantines those rows.
 */
public interface SourcePlugin {

    String CORRUPT_RECORD_COLUMN = "_corrupt_record";

    /** Plugin name, e.g. {@code csv}. Must match the name the job asks for. */
    String name();

    /** Plugin version, e.g. {@code 1.0.0}. Must match the version the job asks for. */
    String version();

    Dataset<Row> read(SparkSession spark, PluginConfig config);
}
