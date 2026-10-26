package com.greensalmon.connectors.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Row and schema rules applied by the core to every plugin's DataFrame. */
public final class ValidationRules {

    private final List<String> requiredColumns;
    private final List<String> notNull;
    private final Map<String, String> types;
    private final double maxBadRowFraction;

    public ValidationRules(List<String> requiredColumns, List<String> notNull, Map<String, String> types,
                           double maxBadRowFraction) {
        if (maxBadRowFraction < 0 || maxBadRowFraction > 1) {
            throw new IllegalArgumentException("max_bad_row_fraction must be between 0 and 1");
        }
        this.requiredColumns = Collections.unmodifiableList(new ArrayList<String>(requiredColumns));
        this.notNull = Collections.unmodifiableList(new ArrayList<String>(notNull));
        this.types = Collections.unmodifiableMap(new LinkedHashMap<String, String>(types));
        this.maxBadRowFraction = maxBadRowFraction;
    }

    public static ValidationRules none() {
        return new ValidationRules(Collections.<String>emptyList(), Collections.<String>emptyList(),
                Collections.<String, String>emptyMap(), 1.0);
    }

    /** Columns that must exist in the DataFrame; a missing one fails the whole run. */
    public List<String> requiredColumns() { return requiredColumns; }

    /** Columns whose value must be present and non-blank; a violation quarantines the row. */
    public List<String> notNull() { return notNull; }

    /** Column to Spark SQL type; a value that does not convert quarantines the row. */
    public Map<String, String> types() { return types; }

    /** Above this share of bad rows the run fails and nothing is written to the target. */
    public double maxBadRowFraction() { return maxBadRowFraction; }
}
