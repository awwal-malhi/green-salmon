package com.greensalmon.shared.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Writes events as single-line JSON through SLF4J (on Databricks: the driver log). */
public final class Slf4jLogSink implements LogSink {

    private final Logger logger;

    public Slf4jLogSink() {
        this("connector.events");
    }

    public Slf4jLogSink(String loggerName) {
        this.logger = LoggerFactory.getLogger(loggerName);
    }

    @Override
    public void write(LogEvent event) {
        String json = event.toJson();
        switch (event.level()) {
            case DEBUG: logger.debug(json); break;
            case WARN: logger.warn(json); break;
            case ERROR: logger.error(json); break;
            default: logger.info(json); break;
        }
    }
}
