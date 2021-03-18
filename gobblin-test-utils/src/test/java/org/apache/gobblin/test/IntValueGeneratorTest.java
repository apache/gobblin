package org.apache.gobblin.test;

import org.testng.annotations.Test;


public class IntValueGeneratorTest {

  @Test
  public void testBuilderForConstant() {
    IntValueGenerator generator = IntValueGenerator.builder()
        .generatorAlgo(GeneratorAlgo.CONSTANT)
        .initValue(1)
        .build();
    for (int i = 0; i < 10; i++) {
      System.out.print(generator.get());
    }
  }
}
