package org.apache.gobblin.test;

import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


public interface IntegerValueGenerator extends ValueGenerator<Integer> {

  @Override
  default Type getLogicalType() {
    return Type.Integer;
  }
}
