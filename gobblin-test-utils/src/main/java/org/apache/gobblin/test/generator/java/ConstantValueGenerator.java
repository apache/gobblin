package org.apache.gobblin.test.generator.java;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;

import lombok.Builder;

import org.apache.gobblin.test.ConfigurableValueGenerator;
import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.config.ConstantValueGeneratorConfig;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(name="Constant",
    targetTypes = {Type.Boolean, Type.Integer, Type.Long, Type.String},
    targetFormat = InMemoryFormat.POJO,
    configClass = ConstantValueGeneratorConfig.class)
public class ConstantValueGenerator<T> implements ValueGenerator<T> {

  private final Type logicalType;
  T initValue;

  @Builder
  ConstantValueGenerator(T initValue) {
    if (initValue instanceof Integer) {
      logicalType = Type.Integer;
    }
    else if (initValue instanceof String) {
      logicalType = Type.String;
    }
    else if (initValue instanceof Boolean) {
      logicalType = Type.Boolean;
    }
    else if (initValue instanceof Long) {
      logicalType = Type.Long;
    }
    else {
      throw new RuntimeException(initValue + " is not part of the supported types");
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
  public Object getPhysicalType() {
    return JavaTypes.getPhysicalType(this.logicalType);
  }

  @Override
  public T get() {
    return initValue;
  }
}
