package com.greensalmon.connectors.core;

import com.greensalmon.connectors.api.PluginConfig;
import com.greensalmon.connectors.api.SourcePlugin;
import com.greensalmon.shared.logging.ConnectorLogger;
import com.greensalmon.shared.notifications.Notification;
import com.greensalmon.shared.notifications.Notifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static com.greensalmon.shared.logging.ConnectorLogger.fields;

/**
 * Stand-in for the in-house Java core: takes a plugin's DataFrame, validates it, quarantines bad
 * rows, writes good rows to Delta, records the run, logs and notifies. Plugins only read.
 */
public final class ConnectorRunner {

    public static final String CORE_VERSION = "0.1.0";

    private static final StructType RUNS_SCHEMA = new StructType()
            .add("run_id", DataTypes.StringType)
            .add("plugin_name", DataTypes.StringType)
            .add("plugin_version", DataTypes.StringType)
            .add("plugin_kind", DataTypes.StringType)
            .add("core_version", DataTypes.StringType)
            .add("target_table", DataTypes.StringType)
            .add("quarantine_table", DataTypes.StringType)
            .add("status", DataTypes.StringType)
            .add("rows_read", DataTypes.LongType)
            .add("rows_written", DataTypes.LongType)
            .add("rows_quarantined", DataTypes.LongType)
            .add("started_at", DataTypes.TimestampType)
            .add("finished_at", DataTypes.TimestampType)
            .add("message", DataTypes.StringType);

    private final ConnectorLogger logger;
    private final Notifier notifier;

    public ConnectorRunner(ConnectorLogger logger, Notifier notifier) {
        this.logger = logger;
        this.notifier = notifier;
    }

    /** Java plugin path: the core calls the plugin, then processes its DataFrame. */
    public RunResult run(final SparkSession spark, final SourcePlugin plugin, final RunRequest request) {
        return execute(spark, request, new SourceReader() {
            @Override
            public Dataset<Row> read() {
                if (!plugin.name().equals(request.pluginName()) || !plugin.version().equals(request.pluginVersion())) {
                    throw new IllegalStateException("Requested plugin " + request.pluginName() + " "
                            + request.pluginVersion() + " but got " + plugin.name() + " " + plugin.version());
                }
                return plugin.read(spark, new PluginConfig(request.source()));
            }
        });
    }

    /** Python plugin path: the plugin already produced the DataFrame. */
    public RunResult runDataFrame(SparkSession spark, final Dataset<Row> source, RunRequest request) {
        return execute(spark, request, new SourceReader() {
            @Override
            public Dataset<Row> read() {
                return source;
            }
        });
    }

    /** Produces the source DataFrame. Anything it throws is handled as a failed run. */
    public interface SourceReader {
        Dataset<Row> read();
    }

    /** General entry point: {@code reader} is called inside the core's error handling. */
    public RunResult run(SparkSession spark, RunRequest request, SourceReader reader) {
        return execute(spark, request, reader);
    }

    private RunResult execute(SparkSession spark, RunRequest request, SourceReader reader) {
        Instant startedAt = Instant.now();
        ConnectorLogger log = logger.with("run_id", request.runId())
                .with("plugin", request.pluginName())
                .with("plugin_version", request.pluginVersion())
                .with("plugin_kind", request.pluginKind());
        log.info("run_started", fields("target_table", request.targetTable(), "core_version", CORE_VERSION));

        RunStatus status;
        String message;
        long read = 0;
        long written = 0;
        long quarantined = 0;
        Dataset<Row> annotated = null;
        try {
            Validator validator = new Validator(request.rules());
            annotated = validator.annotate(reader.read()).persist(StorageLevel.MEMORY_AND_DISK());
            read = annotated.count();

            Dataset<Row> bad = validator.invalidRows(annotated)
                    .withColumn("_run_id", functions.lit(request.runId()))
                    .withColumn("_plugin", functions.lit(request.pluginName()))
                    .withColumn("_plugin_version", functions.lit(request.pluginVersion()))
                    .withColumn("_target_table", functions.lit(request.targetTable()))
                    .withColumn("_quarantined_at", functions.current_timestamp());
            Dataset<Row> good = validator.validRows(annotated)
                    .withColumn("_run_id", functions.lit(request.runId()))
                    .withColumn("_plugin", functions.lit(request.pluginName()))
                    .withColumn("_plugin_version", functions.lit(request.pluginVersion()))
                    .withColumn("_ingested_at", functions.current_timestamp());

            quarantined = bad.count();
            long valid = good.count();
            if (valid + quarantined != read) {
                throw new IllegalStateException("Row reconciliation failed: read " + read + ", valid " + valid
                        + ", quarantined " + quarantined);
            }
            log.info("rows_validated", fields("rows_read", read, "rows_valid", valid, "rows_quarantined", quarantined));

            if (quarantined > 0) {
                bad.write().format("delta").mode("append").saveAsTable(request.quarantineTable());
                log.warn("rows_quarantined", fields("rows_quarantined", quarantined,
                        "quarantine_table", request.quarantineTable()));
            }

            double badFraction = read == 0 ? 0.0 : (double) quarantined / read;
            if (badFraction > request.rules().maxBadRowFraction()) {
                status = RunStatus.FAILED;
                message = String.format("%d of %d rows invalid (%.1f%%), above max_bad_row_fraction %.3f; "
                                + "nothing written to %s, bad rows are in %s",
                        quarantined, read, badFraction * 100, request.rules().maxBadRowFraction(),
                        request.targetTable(), request.quarantineTable());
            } else {
                good.write().format("delta").mode(request.writeMode()).saveAsTable(request.targetTable());
                written = valid;
                status = quarantined > 0 ? RunStatus.SUCCEEDED_WITH_ERRORS : RunStatus.SUCCEEDED;
                message = quarantined > 0
                        ? String.format("%d rows written, %d rows quarantined in %s", written, quarantined,
                        request.quarantineTable())
                        : String.format("%d rows written", written);
            }
        } catch (RuntimeException e) {
            status = RunStatus.FAILED;
            message = e.getClass().getSimpleName() + ": " + e.getMessage();
            written = 0;
            log.error("run_error", fields("error", message));
        } finally {
            if (annotated != null) {
                annotated.unpersist();
            }
        }

        RunResult result = new RunResult(request.runId(), request.pluginName(), request.pluginVersion(),
                request.pluginKind(), request.targetTable(), request.quarantineTable(), status, read, written,
                quarantined, startedAt, Instant.now(), message);
        recordRun(spark, request, result, log);
        log.log(status == RunStatus.FAILED ? com.greensalmon.shared.logging.LogEvent.Level.ERROR
                : com.greensalmon.shared.logging.LogEvent.Level.INFO, "run_finished", result.toMap());
        notify(result, log);
        return result;
    }

    private void recordRun(SparkSession spark, RunRequest request, RunResult r, ConnectorLogger log) {
        if (request.runsTable() == null) {
            return;
        }
        try {
            Row row = RowFactory.create(r.runId(), r.pluginName(), r.pluginVersion(), r.pluginKind(), CORE_VERSION,
                    r.targetTable(), r.quarantineTable(), r.status().name(), r.rowsRead(), r.rowsWritten(),
                    r.rowsQuarantined(), Timestamp.from(r.startedAt()), Timestamp.from(r.finishedAt()), r.message());
            spark.createDataFrame(Collections.singletonList(row), RUNS_SCHEMA)
                    .write().format("delta").mode("append").saveAsTable(request.runsTable());
        } catch (RuntimeException e) {
            log.error("run_record_failed", fields("runs_table", request.runsTable(), "error", e.getMessage()));
        }
    }

    private void notify(RunResult r, ConnectorLogger log) {
        Notification.Severity severity;
        switch (r.status()) {
            case FAILED: severity = Notification.Severity.ERROR; break;
            case SUCCEEDED_WITH_ERRORS: severity = Notification.Severity.WARNING; break;
            default: severity = Notification.Severity.INFO; break;
        }
        String subject = "Connector " + r.pluginName() + " " + r.pluginVersion() + " " + r.status().name();
        Map<String, Object> attributes = r.toMap();
        try {
            notifier.send(new Notification(Instant.now(), severity, subject, r.message(), attributes));
        } catch (RuntimeException e) {
            log.error("notification_failed", fields("error", e.getMessage()));
        }
    }
}
