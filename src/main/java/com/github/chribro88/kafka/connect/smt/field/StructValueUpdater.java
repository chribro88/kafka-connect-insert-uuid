package com.github.chribro88.kafka.connect.smt.field;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

@FunctionalInterface
public interface StructValueUpdater {

    void apply(Struct originalParent, Field originalField, Struct updatedParent, Field updatedField, SingleFieldPath fieldPath);
}