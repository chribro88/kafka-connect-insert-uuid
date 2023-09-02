package com.github.chribro88.kafka.connect.smt.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;

public class ListValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String name, Object value) {
        // if (value == null || ((List) value).isEmpty()) {
        //     throw new ConfigException(name, value, "Empty list");
        // }
    }

    @Override
    public String toString() {
        return "optional list";
    }

}
