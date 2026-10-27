package com.greensalmon.connectors.core;

/** The DataFrame as a whole is unusable (e.g. a required column is missing). Fails the run. */
public final class SchemaValidationException extends RuntimeException {

    public SchemaValidationException(String message) {
        super(message);
    }
}
