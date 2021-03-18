package org.apache.gobblin.test;

import lombok.Builder;

import org.apache.gobblin.test.generator.java.BaseValueGenerator;
import org.apache.gobblin.test.type.Type;


public class IntValueGenerator extends BaseValueGenerator<Integer> implements IntegerValueGenerator {

  private static IntegerValueGenerator getGenerator(GeneratorAlgo generatorAlgo, Integer initValue) {
    switch (generatorAlgo) {
      case CONSTANT: {
        return () -> initValue;
      }
      case RANDOM: {
        return () -> TestUtils.generateRandomInteger();
      }
      case SEQUENTIAL: {
        return new IntegerValueGenerator() {
          private Integer currentValue = initValue;
          @Override
          public Integer get() {
            return currentValue++;
          }
        };
      }
      default: {
        throw new RuntimeException(generatorAlgo.name() + " generation algorithm is not implemented for String Value generation");
      }
    }
  }


  @Builder
  IntValueGenerator(GeneratorAlgo generatorAlgo, Integer initValue) {
    super(Type.Integer, getGenerator(generatorAlgo, initValue));
  }

  @Override
  public Type getLogicalType() {
    return Type.Integer;
  }

}
