package com.greensalmon.shared.logging;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectorLoggerTest {

    @Test
    void contextFieldsAreAddedToEveryEvent() {
        InMemoryLogSink sink = new InMemoryLogSink();
        ConnectorLogger logger = new ConnectorLogger("test", sink).with("run_id", "r1").with("plugin", "csv");

        logger.info("rows_read", ConnectorLogger.fields("count", 3));
        logger.warn("rows_quarantined", ConnectorLogger.fields("count", 1));

        List<LogEvent> events = sink.events();
        assertEquals(2, events.size());
        assertEquals("r1", events.get(0).fields().get("run_id"));
        assertEquals(3, events.get(0).fields().get("count"));
        assertEquals(LogEvent.Level.WARN, events.get(1).level());
        String json = events.get(1).toJson();
        assertTrue(json.contains("\"event\":\"rows_quarantined\""), json);
        assertTrue(json.contains("\"plugin\":\"csv\""), json);
    }

    @Test
    void failingSinkDoesNotBreakOtherSinks() {
        InMemoryLogSink sink = new InMemoryLogSink();
        LogSink broken = new LogSink() {
            @Override
            public void write(LogEvent event) {
                throw new IllegalStateException("boom");
            }
        };
        new ConnectorLogger("test", broken, sink).info("still_logged");
        assertEquals(1, sink.events().size());
    }
}
