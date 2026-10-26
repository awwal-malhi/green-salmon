package com.greensalmon.connectors.core;

import com.greensalmon.connectors.api.PluginConfig;
import com.greensalmon.shared.logging.ConnectorLogger;
import com.greensalmon.shared.logging.Slf4jLogSink;
import com.greensalmon.shared.notifications.RecordingNotifier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry points called from the Python launcher over Py4J ({@code spark._jvm}). Plain strings in
 * and out so the Python side needs no knowledge of the Java types. Each call returns the
 * {@link RunResult} as JSON; it does not throw for a failed run (status FAILED).
 */
public final class PyBridge {

    private PyBridge() {
    }

    public static String coreVersion() {
        return ConnectorRunner.CORE_VERSION;
    }

    /** Java plugin: load it from {@code jarPath} in an isolated class loader and run it. */
    public static String runJarPlugin(final SparkSession spark, final String jarPath, String requestJson) {
        final RunRequest request = RunRequest.fromJson(requestJson);
        final List<PluginLoader.LoadedPlugin> loaded = new ArrayList<PluginLoader.LoadedPlugin>();
        try {
            // Loading happens inside the run so a bad or missing JAR is a recorded, notified failure.
            return runner(request).run(spark, request, new ConnectorRunner.SourceReader() {
                @Override
                public Dataset<Row> read() {
                    PluginLoader.LoadedPlugin plugin =
                            PluginLoader.load(Paths.get(jarPath), request.pluginName(), request.pluginVersion());
                    loaded.add(plugin);
                    return plugin.plugin().read(spark, new PluginConfig(request.source()));
                }
            }).toJson();
        } finally {
            for (PluginLoader.LoadedPlugin plugin : loaded) {
                plugin.close();
            }
        }
    }

    /** Python plugin: the DataFrame was produced in Python; validate and write it here. */
    public static String runDataFrame(SparkSession spark, Dataset<Row> df, String requestJson) {
        RunRequest request = RunRequest.fromJson(requestJson);
        return runner(request).runDataFrame(spark, df, request).toJson();
    }

    /** A failure that happened before a DataFrame existed (e.g. a Python plugin raised); recorded and notified. */
    public static String recordFailure(SparkSession spark, String requestJson, final String message) {
        RunRequest request = RunRequest.fromJson(requestJson);
        return runner(request).run(spark, request, new ConnectorRunner.SourceReader() {
            @Override
            public Dataset<Row> read() {
                throw new PluginFailedException(message);
            }
        }).toJson();
    }

    /** Carries a plugin failure reported from Python. */
    public static final class PluginFailedException extends RuntimeException {
        public PluginFailedException(String message) {
            super(message);
        }
    }

    private static ConnectorRunner runner(RunRequest request) {
        RecordingNotifier notifier = request.notificationsDir() == null
                ? new RecordingNotifier()
                : new RecordingNotifier(Paths.get(request.notificationsDir()));
        return new ConnectorRunner(new ConnectorLogger("connector-core", new Slf4jLogSink()), notifier);
    }
}
