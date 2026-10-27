package com.greensalmon.connectors.core;

import com.greensalmon.connectors.api.PluginConfig;
import com.greensalmon.connectors.api.SourcePlugin;
import com.greensalmon.shared.logging.ConnectorLogger;
import com.greensalmon.shared.logging.InMemoryLogSink;
import com.greensalmon.shared.logging.LogEvent;
import com.greensalmon.shared.notifications.Notification;
import com.greensalmon.shared.notifications.RecordingNotifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorRunnerTest {

    private static final StructType SCHEMA = new StructType()
            .add("order_id", DataTypes.StringType)
            .add("amount", DataTypes.StringType);

    private SparkSession spark;
    private InMemoryLogSink logs;
    private RecordingNotifier notifier;
    private ConnectorRunner runner;
    private String prefix;

    @BeforeEach
    void setUp() {
        spark = SparkTestSupport.spark();
        logs = new InMemoryLogSink();
        notifier = new RecordingNotifier();
        runner = new ConnectorRunner(new ConnectorLogger("test", logs), notifier);
        prefix = "t_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    private Dataset<Row> frame(String... idAmountPairs) {
        List<Row> rows = new ArrayList<Row>();
        for (int i = 0; i < idAmountPairs.length; i += 2) {
            rows.add(RowFactory.create(idAmountPairs[i], idAmountPairs[i + 1]));
        }
        return spark.createDataFrame(rows, SCHEMA);
    }

    private RunRequest request(double maxBadFraction) {
        return RunRequest.builder()
                .plugin("test", "1.0.0")
                .pluginKind("java")
                .targetTable(prefix + "_orders")
                .runsTable(prefix + "_runs")
                .rules(new ValidationRules(Arrays.asList("order_id", "amount"), Collections.singletonList("order_id"),
                        Collections.singletonMap("amount", "decimal(10,2)"), maxBadFraction))
                .build();
    }

    private static SourcePlugin plugin(final String name, final String version, final Dataset<Row> df) {
        return new SourcePlugin() {
            @Override public String name() { return name; }
            @Override public String version() { return version; }
            @Override public Dataset<Row> read(SparkSession s, PluginConfig c) {
                if (df == null) {
                    throw new IllegalStateException("source unavailable");
                }
                return df;
            }
        };
    }

    @Test
    void cleanDataIsWrittenWithRunMetadata() {
        RunResult result = runner.run(spark, plugin("test", "1.0.0", frame("1", "1.00", "2", "2.50")), request(0.0));

        assertEquals(RunStatus.SUCCEEDED, result.status());
        assertEquals(2, result.rowsWritten());
        Dataset<Row> target = spark.table(prefix + "_orders");
        assertEquals(2, target.count());
        assertTrue(Arrays.asList(target.columns()).containsAll(
                Arrays.asList("_run_id", "_plugin", "_plugin_version", "_ingested_at")));
        assertEquals(DataTypes.createDecimalType(10, 2), target.schema().apply("amount").dataType());
        assertFalse(spark.catalog().tableExists(prefix + "_orders_quarantine"));
        assertEquals(Notification.Severity.INFO, notifier.recorded().get(0).severity());
    }

    @Test
    void badRowsAreQuarantinedNotDropped() {
        RunResult result = runner.run(spark,
                plugin("test", "1.0.0", frame("1", "1.00", null, "2.00", "3", "abc", "4", "4.00")), request(0.5));

        assertEquals(RunStatus.SUCCEEDED_WITH_ERRORS, result.status());
        assertEquals(4, result.rowsRead());
        assertEquals(2, result.rowsWritten());
        assertEquals(2, result.rowsQuarantined());
        assertEquals(2, spark.table(prefix + "_orders").count());

        Dataset<Row> quarantine = spark.table(prefix + "_orders_quarantine");
        assertEquals(2, quarantine.count());
        List<String> reasons = new ArrayList<String>();
        for (Row row : quarantine.collectAsList()) {
            reasons.addAll(row.<String>getList(row.fieldIndex("_error_reasons")));
            assertEquals(result.runId(), row.getAs("_run_id"));
        }
        assertTrue(reasons.contains("not_null:order_id"), reasons.toString());
        assertTrue(reasons.contains("invalid_type:amount:decimal(10,2)"), reasons.toString());
        assertEquals(Notification.Severity.WARNING, notifier.recorded().get(0).severity());
        assertTrue(hasEvent("rows_quarantined"));
    }

    @Test
    void tooManyBadRowsFailsAndWritesNothingToTarget() {
        RunResult result = runner.run(spark,
                plugin("test", "1.0.0", frame("1", "x", "2", "y", "3", "3.00")), request(0.1));

        assertEquals(RunStatus.FAILED, result.status());
        assertEquals(0, result.rowsWritten());
        assertFalse(spark.catalog().tableExists(prefix + "_orders"));
        assertEquals(2, spark.table(prefix + "_orders_quarantine").count());
        assertEquals(Notification.Severity.ERROR, notifier.recorded().get(0).severity());
        assertTrue(result.message().contains("max_bad_row_fraction"), result.message());
    }

    @Test
    void pluginExceptionIsAFailedRunThatIsRecordedAndNotified() {
        RunResult result = runner.run(spark, plugin("test", "1.0.0", null), request(0.0));

        assertEquals(RunStatus.FAILED, result.status());
        assertTrue(result.message().contains("source unavailable"), result.message());
        assertEquals(Notification.Severity.ERROR, notifier.recorded().get(0).severity());
        Row run = spark.table(prefix + "_runs").first();
        assertEquals("FAILED", run.getAs("status"));
        assertEquals(result.runId(), run.getAs("run_id"));
        assertTrue(hasEvent("run_error"));
    }

    @Test
    void missingColumnFailsTheRun() {
        StructType schema = new StructType().add("order_id", DataTypes.StringType);
        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(RowFactory.create("1")), schema);
        RunResult result = runner.run(spark, plugin("test", "1.0.0", df), request(0.0));
        assertEquals(RunStatus.FAILED, result.status());
        assertTrue(result.message().contains("SchemaValidationException"), result.message());
    }

    @Test
    void pluginMustBeTheRequestedVersion() {
        RunResult result = runner.run(spark, plugin("test", "2.0.0", frame("1", "1")), request(0.0));
        assertEquals(RunStatus.FAILED, result.status());
        assertTrue(result.message().contains("2.0.0"), result.message());
    }

    @Test
    void overwriteModeReplacesTargetContents() {
        RunRequest append = request(0.0);
        runner.runDataFrame(spark, frame("1", "1", "2", "2"), append);
        RunRequest overwrite = RunRequest.builder().plugin("test", "1.0.0").targetTable(append.targetTable())
                .writeMode("overwrite").rules(append.rules()).build();
        runner.runDataFrame(spark, frame("3", "3"), overwrite);
        assertEquals(1, spark.table(append.targetTable()).count());
    }

    @Test
    void everyRunIsRecordedInTheRunsTable() {
        runner.runDataFrame(spark, frame("1", "1"), request(0.0));
        runner.runDataFrame(spark, frame("1", "bad"), request(0.0));
        List<Row> runs = spark.table(prefix + "_runs").orderBy("started_at").collectAsList();
        assertEquals(2, runs.size());
        assertEquals("SUCCEEDED", runs.get(0).getAs("status"));
        assertEquals("FAILED", runs.get(1).getAs("status"));
        assertEquals(ConnectorRunner.CORE_VERSION, runs.get(0).getAs("core_version"));
    }

    @Test
    void parsesRunConfigJson() {
        RunRequest r = RunRequest.fromJson("{\"plugin\":{\"name\":\"csv\",\"version\":\"1.0.0\",\"kind\":\"java\"},"
                + "\"source\":{\"path\":\"/tmp/x\"},\"target\":{\"table\":\"c.s.orders\"},"
                + "\"validation\":{\"not_null\":[\"id\"],\"types\":{\"id\":\"int\"},\"max_bad_row_fraction\":0.25}}");
        assertEquals("c.s.orders_quarantine", r.quarantineTable());
        assertEquals("append", r.writeMode());
        assertEquals("/tmp/x", r.source().get("path"));
        assertEquals(0.25, r.rules().maxBadRowFraction(), 1e-9);
        assertEquals("int", r.rules().types().get("id"));
        assertFalse(r.runId().isEmpty());
    }

    private boolean hasEvent(String name) {
        for (LogEvent e : logs.events()) {
            if (e.event().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
