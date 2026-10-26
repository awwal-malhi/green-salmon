package com.greensalmon.shared.notifications;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A notification about a connector run. */
public final class Notification {

    public enum Severity { INFO, WARNING, ERROR }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Instant createdAt;
    private final Severity severity;
    private final String subject;
    private final String body;
    private final Map<String, Object> attributes;

    public Notification(Instant createdAt, Severity severity, String subject, String body, Map<String, Object> attributes) {
        this.createdAt = createdAt;
        this.severity = severity;
        this.subject = subject;
        this.body = body;
        this.attributes = attributes == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, Object>(attributes));
    }

    public Instant createdAt() { return createdAt; }
    public Severity severity() { return severity; }
    public String subject() { return subject; }
    public String body() { return body; }
    public Map<String, Object> attributes() { return attributes; }

    public String toJson() {
        Map<String, Object> out = new LinkedHashMap<String, Object>();
        out.put("created_at", createdAt.toString());
        out.put("severity", severity.name());
        out.put("subject", subject);
        out.put("body", body);
        out.put("attributes", attributes);
        try {
            return MAPPER.writeValueAsString(out);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialise notification", e);
        }
    }
}
