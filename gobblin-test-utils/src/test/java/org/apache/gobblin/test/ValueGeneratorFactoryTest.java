package org.apache.gobblin.test;

import java.util.Properties;

import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import org.apache.gobblin.test.generator.FieldConfig;
import org.apache.gobblin.test.generator.ValueGenerator;
import org.apache.gobblin.test.generator.ValueGeneratorFactory;
import org.apache.gobblin.test.type.Type;


public class ValueGeneratorFactoryTest {

  @Test
  public void testStringValueGenerator()
      throws ClassNotFoundException {
    Properties properties = new Properties();
    properties.setProperty("value", "42");
    FieldConfig fieldConfig = new FieldConfig();
    fieldConfig.setType(Type.String);
    fieldConfig.setValueGenConfig(
        ConfigFactory.empty().withValue("value", ConfigValueFactory.fromAnyRef("42")));
    ValueGenerator valueGenerator = ValueGeneratorFactory.getInstance()
        .getValueGenerator("constant", fieldConfig);
    System.out.print(valueGenerator.get());
  }
}