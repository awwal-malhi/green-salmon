package com.greensalmon.connectors.core;

public enum RunStatus {
    /** All rows valid and written. */
    SUCCEEDED,
    /** Valid rows written; some rows quarantined, within the allowed fraction. */
    SUCCEEDED_WITH_ERRORS,
    /** Nothing written to the target. Bad rows (if any were identified) are still quarantined. */
    FAILED
}
