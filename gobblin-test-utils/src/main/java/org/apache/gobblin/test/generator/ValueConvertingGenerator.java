package org.apache.gobblin.test.generator;

import java.util.function.Function;

import org.apache.gobblin.test.type.Type;


public class ValueConvertingGenerator<I, O> implements ValueGenerator<O> {

  private final Function<I,O> function;
  private final ValueGenerator<I> wrappedValueGenerator;

  public ValueConvertingGenerator(ValueGenerator<I> wrappedValueGenerator, Function<I, O> function) {
    this.wrappedValueGenerator = wrappedValueGenerator;
    this.function = function;
  }

  @Override
  public final Type getLogicalType() {
    return wrappedValueGenerator.getLogicalType();
  }

  @Override
  public Object getPhysicalType() {
    return wrappedValueGenerator.getPhysicalType();
  }

  @Override
  public final O get() {
    I innerValue = wrappedValueGenerator.get();
    if (innerValue != null) {
      return function.apply(innerValue);
    } else {
      return null;
    }
  }
}
