package org.apache.gobblin.test;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.test.generator.OptionalValueGenerator;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.type.Type;


public class OptionalValueGeneratorTest {


  @Test
  public void testOptionalValueGenerationAllNulls() {
    Properties configProps = new Properties();
    configProps.setProperty("nullPercentage", "100");
    Config hoconConfig = ConfigFactory.parseProperties(configProps);

    OptionalValueGenerator optionalValueGenerator = new OptionalValueGenerator(new ValueGenerator() {
      @Override
      public Type getLogicalType() {
        return Type.Integer;
      }

      @Override
      public Object get() {
        return 42;
      }
    }, hoconConfig);

    for (int i = 0; i <= 1000; ++i) {
      Object value = optionalValueGenerator.get();
      Assert.assertTrue(value == null);
    }
  }

    @Test
    public void testOptionalValueGenerationNoNulls() {
      Properties configProps = new Properties();

      configProps.setProperty("nullPercentage", "0");
      Config hoconConfig = ConfigFactory.parseProperties(configProps);

      OptionalValueGenerator<Integer> optionalValueGenerator = new OptionalValueGenerator<>(new ValueGenerator<Integer>() {
      @Override
      public Type getLogicalType() {
        return Type.Integer;
      }

      @Override
      public Integer get() {
        return 42;
      }
    }, hoconConfig);

    for (int i = 0; i <= 1000; ++i) {
      Integer value = optionalValueGenerator.get();
      Assert.assertNotNull(value);
    }


  }


}
