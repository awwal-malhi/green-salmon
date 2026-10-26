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

    private static Dataset<Row> read(Path dir, String content, Map<String, Object> extra) throws Exception {
        Path file = dir.resolve("orders.csv");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        Map<String, Object> map = new HashMap<String, Object>(extra);
        map.put("path", file.toString());
        return new CsvSourcePlugin().read(spark, new PluginConfig(map));
    }

    @Test
    void trimsValuesSkipsCommentsAndAddsSourceFile(@TempDir Path dir) throws Exception {
        Dataset<Row> df = read(dir, "order_id,amount\n# exported by test\n 1 , 10.00 \n2,20.00,extra\n",
                new HashMap<String, Object>());

        List<Row> rows = df.orderBy("order_id").collectAsList();
        assertEquals(2, rows.size());
        assertEquals("1", rows.get(0).getAs("order_id"));
        assertEquals("10.00", rows.get(0).getAs("amount"));
        assertNull(rows.get(0).getAs(SourcePlugin.CORRUPT_RECORD_COLUMN));
        assertNotNull(rows.get(1).getAs(SourcePlugin.CORRUPT_RECORD_COLUMN));
        String source = rows.get(0).getAs(CsvSourcePlugin.SOURCE_FILE_COLUMN);
        assertTrue(source.endsWith("orders.csv"), source);
    }

    @Test
    void trimCanBeSwitchedOff(@TempDir Path dir) throws Exception {
        Map<String, Object> extra = new HashMap<String, Object>();
        extra.put("trim", false);
        Row row = read(dir, "order_id,amount\n 1 ,10.00\n", extra).first();
        assertEquals(" 1 ", row.getAs("order_id"));
    }

    @Test
    void isDiscoverableWithServiceLoader() {
        SourcePlugin plugin = ServiceLoader.load(SourcePlugin.class).iterator().next();
        assertEquals("csv", plugin.name());
        assertEquals("2.0.0", plugin.version());
    }
}
