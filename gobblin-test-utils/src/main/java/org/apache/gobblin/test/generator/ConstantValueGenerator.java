package org.apache.gobblin.test.generator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

import lombok.Builder;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(name="Constant",
    targetTypes = {Type.Boolean, Type.Integer, Type.Long, Type.String},
    configuration = ConstantValueGeneratorConfig.class)
public class ConstantValueGenerator<T> implements ValueGenerator<T> {

  Type logicalType;
  T initValue;

  @Builder
  ConstantValueGenerator(T initValue) {
    if (initValue instanceof Integer) {
      logicalType = Type.Integer;
    }
    if (initValue instanceof String) {
      logicalType = Type.String;
    }
    this.initValue = initValue;
  }

  public ConstantValueGenerator(ConstantValueGeneratorConfig<T> config) {
    this(config.getValue());
  }

  public ConstantValueGenerator(Config hoconConfig) {
    this(ConfigBeanFactory.create(hoconConfig, ConstantValueGeneratorConfig.class));
  }

  @Override
  public Type getLogicalType() {
    return this.logicalType;
  }

  @Override
  public T get() {
    return initValue;
  }
}
