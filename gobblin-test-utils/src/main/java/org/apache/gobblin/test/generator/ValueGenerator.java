package org.apache.gobblin.test.generator;

import java.util.function.Supplier;

import org.apache.gobblin.test.type.Type;


public interface ValueGenerator<T> extends Supplier<T> {

  /**
   * Describe the logical type of the value that this generator generates
   * @return
   */
  Type getLogicalType();

}
