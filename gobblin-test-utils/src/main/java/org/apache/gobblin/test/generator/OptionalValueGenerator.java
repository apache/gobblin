package org.apache.gobblin.test.generator;

import com.google.common.base.Preconditions;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.test.TestUtils;


public class OptionalValueGenerator<T> extends ValueConvertingGenerator<T, T> {

  public OptionalValueGenerator(ValueGenerator valueGenerator, com.typesafe.config.Config config) {
    this(valueGenerator, ConfigBeanFactory.create(config, OptionalValueGeneratorConfig.class));
  }

  public OptionalValueGenerator(ValueGenerator<T> valueGenerator, OptionalValueGeneratorConfig config) {
    super(valueGenerator, input -> {
      if (TestUtils.generateRandomInteger(100) < config.getNullPercentage()) {
        return null;
      } else {
        return input;
      }
    });
    Preconditions.checkArgument(config.getNullPercentage() >= 0 && config.getNullPercentage() <= 100);
  }

  public OptionalValueGenerator(ValueGenerator<T> valueGenerator) {
    this(valueGenerator, ConfigFactory.empty());
  }

}
