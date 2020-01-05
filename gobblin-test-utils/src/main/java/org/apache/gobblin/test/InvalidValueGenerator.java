package org.apache.gobblin.test;

import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


public class InvalidValueGenerator implements ValueGenerator {
  private Type type;
  InvalidValueGenerator(Type logicalType) {
    type = logicalType;
  }

  @Override
  public Type getLogicalType() {
    return type;
  }

  @Override
  public Object get() {
    throw new RuntimeException("Failed to generate value because a valid value generator has not been configured");
  }
}
