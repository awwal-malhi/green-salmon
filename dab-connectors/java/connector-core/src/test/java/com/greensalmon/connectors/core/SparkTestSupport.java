package com.greensalmon.connectors.core;

import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** One local Spark session with Delta for all tests in the JVM. */
final class SparkTestSupport {

    private static SparkSession spark;

    private SparkTestSupport() {
    }

    static synchronized SparkSession spark() {
        if (spark == null) {
            Path warehouse;
            try {
                warehouse = Files.createTempDirectory("connector-core-warehouse");
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            spark = SparkSession.builder()
                    .master("local[2]")
                    .appName("connector-core-tests")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                    .config("spark.sql.warehouse.dir", warehouse.toString())
                    .config("spark.sql.shuffle.partitions", "2")
                    .config("spark.ui.enabled", "false")
                    .getOrCreate();
            spark.sparkContext().setLogLevel("WARN");
        }
        return spark;
    }
}
