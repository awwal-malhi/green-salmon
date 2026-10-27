package com.greensalmon.connectors.core;

import com.greensalmon.connectors.api.SourcePlugin;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Applies {@link ValidationRules}. Each row gets an {@code _error_reasons} array; rows with an empty
 * array are valid, all others are quarantined. Nothing is filtered out without a reason.
 */
public final class Validator {

    public static final String ERRORS_COLUMN = "_error_reasons";
    public static final String RECORD_COLUMN = "_record";

    private final ValidationRules rules;

    public Validator(ValidationRules rules) {
        this.rules = rules;
        for (Map.Entry<String, String> type : rules.types().entrySet()) {
            try {
                DataType.fromDDL(type.getValue());
            } catch (Exception e) {
                // Spark's ParseException is not a RuntimeException in Java's eyes.
                throw new IllegalArgumentException(
                        "validation.types: '" + type.getValue() + "' for column '" + type.getKey()
                                + "' is not a Spark SQL type", e);
            }
        }
    }

    /** Checks the schema, then adds {@link #ERRORS_COLUMN}. */
    public Dataset<Row> annotate(Dataset<Row> df) {
        Set<String> present = new LinkedHashSet<String>(Arrays.asList(df.columns()));
        Set<String> needed = new LinkedHashSet<String>(rules.requiredColumns());
        needed.addAll(rules.notNull());
        needed.addAll(rules.types().keySet());
        List<String> missing = new ArrayList<String>();
        for (String column : needed) {
            if (!present.contains(column)) {
                missing.add(column);
            }
        }
        if (!missing.isEmpty()) {
            throw new SchemaValidationException("Missing required columns " + missing + "; source has " + present);
        }

        List<Column> checks = new ArrayList<Column>();
        if (present.contains(SourcePlugin.CORRUPT_RECORD_COLUMN)) {
            checks.add(functions.when(col(SourcePlugin.CORRUPT_RECORD_COLUMN).isNotNull(), functions.lit("malformed_record")));
        }
        for (String column : rules.notNull()) {
            checks.add(functions.when(isBlank(column), functions.lit("not_null:" + column)));
        }
        for (Map.Entry<String, String> type : rules.types().entrySet()) {
            String column = type.getKey();
            checks.add(functions.when(isBlank(column).unary_$bang().and(tryCast(column, type.getValue()).isNull()),
                    functions.lit("invalid_type:" + column + ":" + type.getValue())));
        }
        Column errors = checks.isEmpty()
                ? functions.array().cast("array<string>")
                : functions.array_compact(functions.array(checks.toArray(new Column[0])));
        return df.withColumn(ERRORS_COLUMN, errors);
    }

    /** Valid rows, with typed columns and without the validation helper columns. */
    public Dataset<Row> validRows(Dataset<Row> annotated) {
        Dataset<Row> valid = annotated.filter(functions.size(col(ERRORS_COLUMN)).equalTo(0))
                .drop(ERRORS_COLUMN)
                .drop(SourcePlugin.CORRUPT_RECORD_COLUMN);
        for (Map.Entry<String, String> type : rules.types().entrySet()) {
            valid = valid.withColumn(type.getKey(), tryCast(type.getKey(), type.getValue()));
        }
        return valid;
    }

    /** Invalid rows as {@code _error_reasons} plus the whole original row as JSON in {@code _record}. */
    public Dataset<Row> invalidRows(Dataset<Row> annotated) {
        List<Column> original = new ArrayList<Column>();
        for (String column : annotated.columns()) {
            if (!ERRORS_COLUMN.equals(column)) {
                original.add(col(column));
            }
        }
        return annotated.filter(functions.size(col(ERRORS_COLUMN)).gt(0))
                .select(col(ERRORS_COLUMN),
                        functions.to_json(functions.struct(original.toArray(new Column[0]))).alias(RECORD_COLUMN));
    }

    private static Column col(String name) {
        return functions.col("`" + name.replace("`", "``") + "`");
    }

    private static Column isBlank(String column) {
        return col(column).isNull().or(functions.trim(col(column).cast("string")).equalTo(""));
    }

    /** try_cast keeps working when ANSI mode is on; a failed conversion gives null. */
    private static Column tryCast(String column, String type) {
        return functions.expr("try_cast(`" + column.replace("`", "``") + "` AS " + type + ")");
    }
}
