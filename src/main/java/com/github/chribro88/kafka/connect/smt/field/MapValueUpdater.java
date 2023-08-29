package com.github.chribro88.kafka.connect.smt.field;

import java.util.Map;

@FunctionalInterface
public interface MapValueUpdater {

    /**
     * @param originalParent original data object
     * @param updatedParent data object being updated
     * @param fieldPath if match happened, null if applies to other fields
     * @param fieldName field name
     */
    void apply(
            Map<String, Object> originalParent,
            Map<String, Object> updatedParent,
            SingleFieldPath fieldPath,
            String fieldName
    );
}