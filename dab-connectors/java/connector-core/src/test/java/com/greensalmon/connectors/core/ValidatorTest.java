package com.greensalmon.connectors.core;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValidatorTest {

    private static final StructType SCHEMA = new StructType()
            .add("id", DataTypes.StringType)
            .add("amount", DataTypes.StringType)
            .add("_corrupt_record", DataTypes.StringType);

    private static Dataset<Row> rows(Row... rows) {
        SparkSession spark = SparkTestSupport.spark();
        return spark.createDataFrame(Arrays.asList(rows), SCHEMA);
    }

    private static ValidationRules rules() {
        Map<String, String> types = new LinkedHashMap<String, String>();
        types.put("id", "int");
        types.put("amount", "decimal(10,2)");
        return new ValidationRules(Arrays.asList("id", "amount"), Collections.singletonList("id"), types, 0.5);
    }

    @Test
    void flagsEveryKindOfBadRowWithAReason() {
        Dataset<Row> annotated = new Validator(rules()).annotate(rows(
                RowFactory.create("1", "10.50", null),
                RowFactory.create(null, "1", null),
                RowFactory.create(" ", "1", null),
                RowFactory.create("x", "1", null),
                RowFactory.create("4", "lots", null),
                RowFactory.create("5", "1", "5,1,extra")));

        List<Row> out = annotated.orderBy("amount").collectAsList();
        Map<String, List<String>> reasonsById = new LinkedHashMap<String, List<String>>();
        for (Row row : annotated.collectAsList()) {
            Object id = row.getAs("id");
            reasonsById.put(String.valueOf(id), row.<String>getList(row.fieldIndex(Validator.ERRORS_COLUMN)));
        }
        assertEquals(6, out.size());
        assertEquals(Collections.<String>emptyList(), reasonsById.get("1"));
        assertEquals(Collections.singletonList("not_null:id"), reasonsById.get("null"));
        assertEquals(Collections.singletonList("not_null:id"), reasonsById.get(" "));
        assertEquals(Collections.singletonList("invalid_type:id:int"), reasonsById.get("x"));
        assertEquals(Collections.singletonList("invalid_type:amount:decimal(10,2)"), reasonsById.get("4"));
        assertEquals(Collections.singletonList("malformed_record"), reasonsById.get("5"));
    }

    @Test
    void validRowsAreTypedAndHelperColumnsRemoved() {
        Validator validator = new Validator(rules());
        Dataset<Row> annotated = validator.annotate(rows(
                RowFactory.create("1", "10.50", null),
                RowFactory.create("x", "1", null)));

        Dataset<Row> valid = validator.validRows(annotated);
        assertEquals(Arrays.asList("id", "amount"), Arrays.asList(valid.columns()));
        assertEquals(DataTypes.IntegerType, valid.schema().apply("id").dataType());
        assertEquals(DataTypes.createDecimalType(10, 2), valid.schema().apply("amount").dataType());
        assertEquals(1, valid.count());

        Dataset<Row> invalid = validator.invalidRows(annotated);
        Row bad = invalid.first();
        String record = bad.getAs(Validator.RECORD_COLUMN);
        assertTrue(record.contains("\"id\":\"x\""), record);
        assertFalse(record.contains(Validator.ERRORS_COLUMN), record);
    }

    @Test
    void missingRequiredColumnFailsTheWholeFrame() {
        ValidationRules rules = new ValidationRules(Arrays.asList("id", "customer"), Collections.<String>emptyList(),
                Collections.<String, String>emptyMap(), 0.0);
        SchemaValidationException e = assertThrows(SchemaValidationException.class,
                () -> new Validator(rules).annotate(rows(RowFactory.create("1", "1", null))));
        assertTrue(e.getMessage().contains("customer"), e.getMessage());
    }

    @Test
    void rejectsUnknownTypeNames() {
        ValidationRules rules = new ValidationRules(Collections.<String>emptyList(), Collections.<String>emptyList(),
                Collections.singletonMap("id", "not_a_type"), 0.0);
        assertThrows(IllegalArgumentException.class, () -> new Validator(rules));
    }

    @Test
    void noRulesMeansEveryRowIsValid() {
        Validator validator = new Validator(ValidationRules.none());
        StructType schema = new StructType().add("id", DataTypes.StringType);
        Dataset<Row> df = SparkTestSupport.spark().createDataFrame(
                Arrays.asList(RowFactory.create("a"), RowFactory.create((Object) null)), schema);
        assertEquals(2, validator.validRows(validator.annotate(df)).count());
    }
}
