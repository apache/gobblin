package org.apache.gobblin.test;

import com.typesafe.config.Config;

import lombok.Builder;

import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


@ConfigurableValueGenerator(name="string", targetTypes = Type.String)
public class StringValueGenerator extends BaseValueGenerator<String> implements BaseStringValueGenerator {

  private static ValueGenerator<String> getGenerator(GeneratorAlgo generatorAlgo, String initValue) {
    switch (generatorAlgo) {
      case CONSTANT: {
        return (BaseStringValueGenerator) () -> initValue;
      }
      case RANDOM: {
        return (BaseStringValueGenerator) () -> TestUtils.generateRandomAlphaString(20);
      }
      default: {
        throw new RuntimeException(generatorAlgo.name() + " generation algorithm is not implemented for String Value generation");
      }
    }
  }

  @Builder
  StringValueGenerator(GeneratorAlgo generatorAlgo, String initValue) {
    super(Type.String, getGenerator(generatorAlgo, initValue));
  }


  public StringValueGenerator(Config hoconConfig) {
    this(GeneratorAlgo.RANDOM, "");
  }

}
