package org.apache.gobblin.test.generator;

import lombok.Builder;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(name="sequential",
    targetTypes = {Type.Integer, Type.Long},
    configuration = SequentialValueGeneratorConfig.class)
public class SequentialValueGenerator<T extends Number> implements ValueGenerator<T> {
  private final Type targetType;
  private final T initValue;
  Integer currentIntValue;
  Long currentLongValue;

  @Builder
  SequentialValueGenerator(T initValue, Type targetType) {
    this.initValue = initValue;
    this.targetType = targetType;
    switch (this.targetType) {
      case Integer: {
        this.currentIntValue = (Integer) this.initValue;
        break;
      }
      case Long: {
        this.currentLongValue = (Long) this.initValue;
        break;
      }
      default: throw new RuntimeException("Not implemented");
    }
  }

  public SequentialValueGenerator(SequentialValueGeneratorConfig<T> config) {
    this(config.getInitValue(), config.getFieldConfig().getType());
  }

  @Override
  public Type getLogicalType() {
    return this.targetType;
  }

  @Override
  public T get() {
    switch (this.targetType) {
      case Integer: {
        return (T) this.currentIntValue++;
      }
      case Long: {
        return (T) this.currentLongValue++;
      }
      default: throw new RuntimeException("Not implemented");
    }
  }
}
