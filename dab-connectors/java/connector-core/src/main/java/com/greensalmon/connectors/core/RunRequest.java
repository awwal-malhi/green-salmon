package com.greensalmon.connectors.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Everything the core needs for one run. Parsed from the run config JSON:
 *
 * <pre>
 * {
 *   "run_id": "optional",
 *   "plugin": {"name": "csv", "version": "1.0.0", "kind": "java"},
 *   "source": { ...passed to the plugin unchanged... },
 *   "target": {"table": "cat.schema.orders", "mode": "append",
 *              "quarantine_table": "cat.schema.orders_quarantine", "runs_table": "cat.schema.connector_runs"},
 *   "validation": {"required_columns": [], "not_null": [], "types": {}, "max_bad_row_fraction": 0.1},
 *   "notifications": {"dir": "/Volumes/cat/schema/vol/notifications"}
 * }
 * </pre>
 */
public final class RunRequest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String runId;
    private final String pluginName;
    private final String pluginVersion;
    private final String pluginKind;
    private final Map<String, Object> source;
    private final String targetTable;
    private final String writeMode;
    private final String quarantineTable;
    private final String runsTable;
    private final ValidationRules rules;
    private final String notificationsDir;

    private RunRequest(Builder b) {
        this.runId = b.runId == null || b.runId.isEmpty() ? UUID.randomUUID().toString() : b.runId;
        this.pluginName = require(b.pluginName, "plugin.name");
        this.pluginVersion = require(b.pluginVersion, "plugin.version");
        this.pluginKind = b.pluginKind == null ? "unknown" : b.pluginKind;
        this.source = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(b.source));
        this.targetTable = require(b.targetTable, "target.table");
        this.writeMode = b.writeMode == null ? "append" : b.writeMode;
        if (!"append".equals(writeMode) && !"overwrite".equals(writeMode)) {
            throw new IllegalArgumentException("target.mode must be 'append' or 'overwrite', got '" + writeMode + "'");
        }
        this.quarantineTable = b.quarantineTable == null ? targetTable + "_quarantine" : b.quarantineTable;
        this.runsTable = b.runsTable;
        this.rules = b.rules == null ? ValidationRules.none() : b.rules;
        this.notificationsDir = b.notificationsDir;
    }

    private static String require(String value, String key) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Run config is missing '" + key + "'");
        }
        return value;
    }

    public String runId() { return runId; }
    public String pluginName() { return pluginName; }
    public String pluginVersion() { return pluginVersion; }
    public String pluginKind() { return pluginKind; }
    public Map<String, Object> source() { return source; }
    public String targetTable() { return targetTable; }
    public String writeMode() { return writeMode; }
    public String quarantineTable() { return quarantineTable; }
    /** May be null: then runs are only logged, not recorded in a table. */
    public String runsTable() { return runsTable; }
    public ValidationRules rules() { return rules; }
    /** May be null: then notifications are recorded in memory only. */
    public String notificationsDir() { return notificationsDir; }

    public static Builder builder() {
        return new Builder();
    }

    public static RunRequest fromJson(String json) {
        Map<String, Object> root;
        try {
            root = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
        } catch (IOException e) {
            throw new IllegalArgumentException("Run config is not valid JSON: " + e.getMessage(), e);
        }
        Map<String, Object> plugin = section(root, "plugin");
        Map<String, Object> target = section(root, "target");
        Map<String, Object> validation = section(root, "validation");
        Map<String, Object> notifications = section(root, "notifications");

        Object maxFraction = validation.get("max_bad_row_fraction");
        ValidationRules rules = new ValidationRules(
                strings(validation.get("required_columns"), "validation.required_columns"),
                strings(validation.get("not_null"), "validation.not_null"),
                stringMap(validation.get("types"), "validation.types"),
                maxFraction == null ? 0.0 : ((Number) maxFraction).doubleValue());

        return builder()
                .runId(str(root.get("run_id")))
                .plugin(str(plugin.get("name")), str(plugin.get("version")))
                .pluginKind(str(plugin.get("kind")))
                .source(section(root, "source"))
                .targetTable(str(target.get("table")))
                .writeMode(str(target.get("mode")))
                .quarantineTable(str(target.get("quarantine_table")))
                .runsTable(str(target.get("runs_table")))
                .rules(rules)
                .notificationsDir(str(notifications.get("dir")))
                .build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> root, String key) {
        Object value = root.get(key);
        if (value == null) {
            return Collections.emptyMap();
        }
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Run config section '" + key + "' must be an object");
        }
        return (Map<String, Object>) value;
    }

    private static String str(Object value) {
        return value == null ? null : value.toString();
    }

    private static List<String> strings(Object value, String key) {
        List<String> out = new ArrayList<String>();
        if (value == null) {
            return out;
        }
        if (!(value instanceof List)) {
            throw new IllegalArgumentException("'" + key + "' must be a list");
        }
        for (Object item : (List<?>) value) {
            out.add(String.valueOf(item));
        }
        return out;
    }

    private static Map<String, String> stringMap(Object value, String key) {
        Map<String, String> out = new LinkedHashMap<String, String>();
        if (value == null) {
            return out;
        }
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("'" + key + "' must be an object");
        }
        for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
            out.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
        }
        return out;
    }

    public static final class Builder {
        private String runId;
        private String pluginName;
        private String pluginVersion;
        private String pluginKind;
        private Map<String, Object> source = new LinkedHashMap<String, Object>();
        private String targetTable;
        private String writeMode;
        private String quarantineTable;
        private String runsTable;
        private ValidationRules rules;
        private String notificationsDir;

        public Builder runId(String v) { runId = v; return this; }
        public Builder plugin(String name, String version) { pluginName = name; pluginVersion = version; return this; }
        public Builder pluginKind(String v) { pluginKind = v; return this; }
        public Builder source(Map<String, Object> v) { source = v == null ? new LinkedHashMap<String, Object>() : v; return this; }
        public Builder targetTable(String v) { targetTable = v; return this; }
        public Builder writeMode(String v) { writeMode = v; return this; }
        public Builder quarantineTable(String v) { quarantineTable = v; return this; }
        public Builder runsTable(String v) { runsTable = v; return this; }
        public Builder rules(ValidationRules v) { rules = v; return this; }
        public Builder notificationsDir(String v) { notificationsDir = v; return this; }

        public RunRequest build() {
            return new RunRequest(this);
        }
    }
}
