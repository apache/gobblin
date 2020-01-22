package org.apache.gobblin.test.generator;

import java.util.List;

import org.apache.gobblin.test.generator.config.FieldConfig;


public interface StructValueGenerator<T> extends ValueGenerator<T> {

  List<FieldConfig> getStructSchema();

}
