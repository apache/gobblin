package org.apache.gobblin.test.generator;

import com.typesafe.config.Config;


//@ConfigurableValueGenerator(name="Constant", targetTypes = Type.Integer)
public class ConstantIntValueGenerator extends ConstantValueGenerator<Integer> {
  public ConstantIntValueGenerator(Config hoconConfig) {
    super(hoconConfig);
  }
}
