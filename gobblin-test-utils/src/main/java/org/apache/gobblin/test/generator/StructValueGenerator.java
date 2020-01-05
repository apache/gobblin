package org.apache.gobblin.test.generator;

import java.util.List;


public interface StructValueGenerator<T> extends ValueGenerator<T> {

  List<Field> getStructSchema();

}
