package com.greensalmon.shared.logging;

/** Destination for structured log events. */
public interface LogSink {

    void write(LogEvent event);
}
