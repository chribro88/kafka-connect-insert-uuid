package com.github.chribro88.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import java.util.Objects;

public class ExtendedConfigDef extends ConfigDef {

    public ExtendedConfigDef() {
        super();
    }

    public ExtendedConfigDef addDefinition(ConfigDef toAdd) {
        Objects.requireNonNull(toAdd, "Additional ConfigDef may not be null");
        toAdd.configKeys().values().forEach(this::define);
        return this;
    }

}
