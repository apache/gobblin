package org.apache.gobblin.test;

import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


public interface BaseStringValueGenerator extends ValueGenerator<String> {
  @Override
  default Type getLogicalType() {
    return Type.String;
  }

  @Override
  default Object getPhysicalType() { return String.class; }
}
