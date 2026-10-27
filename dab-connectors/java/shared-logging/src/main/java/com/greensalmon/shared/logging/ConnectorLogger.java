package com.greensalmon.shared.logging;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Structured logger shared by connectors. Every event carries the logger's context fields
 * (e.g. run id, plugin, version) so log lines from one run can be correlated.
 */
public final class ConnectorLogger {

    private final String component;
    private final Map<String, Object> context;
    private final List<LogSink> sinks;
    private final Clock clock;

    public ConnectorLogger(String component, LogSink... sinks) {
        this(component, Collections.<String, Object>emptyMap(), Arrays.asList(sinks), Clock.systemUTC());
    }

    private ConnectorLogger(String component, Map<String, Object> context, List<LogSink> sinks, Clock clock) {
        if (sinks.isEmpty()) {
            throw new IllegalArgumentException("ConnectorLogger needs at least one sink");
        }
        this.component = component;
        this.context = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(context));
        this.sinks = sinks;
        this.clock = clock;
    }

    /** Returns a logger that adds {@code key=value} to every event. */
    public ConnectorLogger with(String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<String, Object>(context);
        next.put(key, value);
        return new ConnectorLogger(component, next, sinks, clock);
    }

    public void info(String event, Map<String, ?> fields) { log(LogEvent.Level.INFO, event, fields); }
    public void warn(String event, Map<String, ?> fields) { log(LogEvent.Level.WARN, event, fields); }
    public void error(String event, Map<String, ?> fields) { log(LogEvent.Level.ERROR, event, fields); }

    public void info(String event) { info(event, Collections.<String, Object>emptyMap()); }

    public void log(LogEvent.Level level, String event, Map<String, ?> fields) {
        Map<String, Object> all = new LinkedHashMap<String, Object>(context);
        if (fields != null) {
            all.putAll(fields);
        }
        LogEvent logEvent = new LogEvent(clock.instant(), level, component, event, all);
        for (LogSink sink : sinks) {
            try {
                sink.write(logEvent);
            } catch (RuntimeException ignored) {
                // A failing log sink must never fail the connector run.
            }
        }
    }

    /** Small helper for building field maps in Java 8. */
    public static Map<String, Object> fields(Object... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("fields() takes key/value pairs");
        }
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return map;
    }
}
