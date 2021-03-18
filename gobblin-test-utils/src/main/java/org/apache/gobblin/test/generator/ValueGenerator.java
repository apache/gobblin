package org.apache.gobblin.test.generator;

import java.util.function.Supplier;

import org.apache.gobblin.test.type.Type;


public interface ValueGenerator<T> extends Supplier<T> {

  /**
   * Describe the logical type of the value that this generator generates
   * @return
   */
  Type getLogicalType();

  /**
   * Describe the physical type of the value that this generator generates
   * Examples would be :
   * a class object for Java pojos,
   * a Schema object for Avro schemas,
   * a ProtoDescriptor for Protobuf classes etc.
   * @return
   */
  Object getPhysicalType();

}
