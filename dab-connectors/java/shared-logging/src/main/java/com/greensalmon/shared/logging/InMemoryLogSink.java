package com.greensalmon.shared.logging;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Keeps events in memory. Intended for tests. */
public final class InMemoryLogSink implements LogSink {

    private final List<LogEvent> events = Collections.synchronizedList(new ArrayList<LogEvent>());

    @Override
    public void write(LogEvent event) {
        events.add(event);
    }

    public List<LogEvent> events() {
        synchronized (events) {
            return new ArrayList<LogEvent>(events);
        }
    }
}
