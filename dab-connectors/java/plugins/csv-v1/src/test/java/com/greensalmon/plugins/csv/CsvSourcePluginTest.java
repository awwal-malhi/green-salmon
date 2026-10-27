package com.greensalmon.plugins.csv;

import com.greensalmon.connectors.api.PluginConfig;
import com.greensalmon.connectors.api.SourcePlugin;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvSourcePluginTest {

    private static SparkSession spark;

    @BeforeAll
    static void start() {
        spark = SparkSession.builder().master("local[1]").config("spark.ui.enabled", "false").getOrCreate();
    }

    @AfterAll
    static void stop() {
        spark.stop();
    }

    private static Path write(Path dir, String content) throws Exception {
        Path file = dir.resolve("orders.csv");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private static PluginConfig config(Path file, Object columns) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("path", file.toString());
        if (columns != null) {
            map.put("columns", columns);
        }
        return new PluginConfig(map);
    }

    @Test
    void readsAllColumnsAsStringsAndFlagsMalformedRows(@TempDir Path dir) throws Exception {
        Path file = write(dir, "order_id,amount\n1,10.00\n2\n3,30.00,extra\n");
        Dataset<Row> df = new CsvSourcePlugin().read(spark, config(file, null));

        assertEquals(Arrays.asList("order_id", "amount", SourcePlugin.CORRUPT_RECORD_COLUMN), Arrays.asList(df.columns()));
        List<Row> rows = df.orderBy("order_id").collectAsList();
        assertEquals(3, rows.size());
        assertNull(rows.get(0).getAs(SourcePlugin.CORRUPT_RECORD_COLUMN));
        assertNotNull(rows.get(1).getAs(SourcePlugin.CORRUPT_RECORD_COLUMN));
        assertNotNull(rows.get(2).getAs(SourcePlugin.CORRUPT_RECORD_COLUMN));
    }

    @Test
    void v1KeepsWhitespaceAndHasNoSourceFileColumn(@TempDir Path dir) throws Exception {
        Path file = write(dir, "order_id,amount\n 1 , 10.00\n");
        Row row = new CsvSourcePlugin().read(spark, config(file, Arrays.asList("order_id", "amount"))).first();
        assertEquals(" 1 ", row.getAs("order_id"));
        assertEquals(3, row.size());
    }

    @Test
    void isDiscoverableWithServiceLoader() {
        SourcePlugin plugin = ServiceLoader.load(SourcePlugin.class).iterator().next();
        assertEquals("csv", plugin.name());
        assertEquals("1.0.0", plugin.version());
        assertTrue(plugin instanceof CsvSourcePlugin);
    }
}
