package com.greensalmon.connectors.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Outcome of one run. */
public final class RunResult {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String runId;
    private final String pluginName;
    private final String pluginVersion;
    private final String pluginKind;
    private final String targetTable;
    private final String quarantineTable;
    private final RunStatus status;
    private final long rowsRead;
    private final long rowsWritten;
    private final long rowsQuarantined;
    private final Instant startedAt;
    private final Instant finishedAt;
    private final String message;

    public RunResult(String runId, String pluginName, String pluginVersion, String pluginKind, String targetTable,
                     String quarantineTable, RunStatus status, long rowsRead, long rowsWritten, long rowsQuarantined,
                     Instant startedAt, Instant finishedAt, String message) {
        this.runId = runId;
        this.pluginName = pluginName;
        this.pluginVersion = pluginVersion;
        this.pluginKind = pluginKind;
        this.targetTable = targetTable;
        this.quarantineTable = quarantineTable;
        this.status = status;
        this.rowsRead = rowsRead;
        this.rowsWritten = rowsWritten;
        this.rowsQuarantined = rowsQuarantined;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.message = message;
    }

    public String runId() { return runId; }
    public String pluginName() { return pluginName; }
    public String pluginVersion() { return pluginVersion; }
    public String pluginKind() { return pluginKind; }
    public String targetTable() { return targetTable; }
    public String quarantineTable() { return quarantineTable; }
    public RunStatus status() { return status; }
    public long rowsRead() { return rowsRead; }
    public long rowsWritten() { return rowsWritten; }
    public long rowsQuarantined() { return rowsQuarantined; }
    public Instant startedAt() { return startedAt; }
    public Instant finishedAt() { return finishedAt; }
    public String message() { return message; }

    public Map<String, Object> toMap() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("run_id", runId);
        out.put("plugin_name", pluginName);
        out.put("plugin_version", pluginVersion);
        out.put("plugin_kind", pluginKind);
        out.put("target_table", targetTable);
        out.put("quarantine_table", quarantineTable);
        out.put("status", status.name());
        out.put("rows_read", rowsRead);
        out.put("rows_written", rowsWritten);
        out.put("rows_quarantined", rowsQuarantined);
        out.put("started_at", startedAt.toString());
        out.put("finished_at", finishedAt.toString());
        out.put("message", message);
        return out;
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(toMap());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
