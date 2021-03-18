package org.apache.gobblin.test.generator.java;

import com.typesafe.config.Config;

import lombok.Builder;

import org.apache.gobblin.test.BaseStringValueGenerator;
import org.apache.gobblin.test.GeneratorAlgo;
import org.apache.gobblin.test.TestUtils;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;

//TODO: Deprecate
@Deprecated
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
  public StringValueGenerator(GeneratorAlgo generatorAlgo, String initValue) {
    super(Type.String, getGenerator(generatorAlgo, initValue));
  }


  public StringValueGenerator(Config hoconConfig) {
    this(GeneratorAlgo.RANDOM, "");
  }

}
