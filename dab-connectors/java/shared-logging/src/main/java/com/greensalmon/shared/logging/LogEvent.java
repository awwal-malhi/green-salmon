package com.greensalmon.shared.logging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** One structured log event: a level, an event name, and key/value fields. */
public final class LogEvent {

    public enum Level { DEBUG, INFO, WARN, ERROR }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Instant timestamp;
    private final Level level;
    private final String component;
    private final String event;
    private final Map<String, Object> fields;

    public LogEvent(Instant timestamp, Level level, String component, String event, Map<String, Object> fields) {
        this.timestamp = timestamp;
        this.level = level;
        this.component = component;
        this.event = event;
        this.fields = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(fields));
    }

    public Instant timestamp() { return timestamp; }
    public Level level() { return level; }
    public String component() { return component; }
    public String event() { return event; }
    public Map<String, Object> fields() { return fields; }

    public String toJson() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("ts", timestamp.toString());
        out.put("level", level.name());
        out.put("component", component);
        out.put("event", event);
        out.putAll(fields);
        try {
            return MAPPER.writeValueAsString(out);
        } catch (JsonProcessingException e) {
            return "{\"event\":\"" + event + "\",\"serialization_error\":\"" + e.getMessage() + "\"}";
        }
    }
}
