package com.github.chribro88.kafka.connect.smt.field;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Function to update Struct schemas based on Field Paths.
 *
 * @see FieldPath
 * @see org.apache.kafka.connect.data.Schema
 */
@FunctionalInterface
public interface StructSchemaUpdater {

    /**
     * Apply schema update function.
     *
     * @param schemaBuilder builder to be updated. Required, not nullable.
     * @param field nullable when updating fields that do not exist. e.g. paths not found.
     * @param fieldPath nullable when updating a field that is not related to a path.
     */
    void apply(SchemaBuilder schemaBuilder, Field field, SingleFieldPath fieldPath);
}