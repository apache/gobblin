package org.apache.gobblin.test.generator.java;

import org.apache.gobblin.test.generator.ValueGenerator;


public abstract class JavaValueGenerator<T> implements ValueGenerator<T> {

  @Override
  public Object getPhysicalType() {
    return JavaTypes.getPhysicalType(getLogicalType());
  }

}
