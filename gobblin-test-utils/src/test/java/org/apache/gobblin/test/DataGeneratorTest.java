package org.apache.gobblin.test;

import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.test.generator.DataGenerator;
import org.apache.gobblin.test.generator.DataGeneratorConfig;


public class DataGeneratorTest {

  @Test
  public void testConfigLoad()
      throws ClassNotFoundException {
    Config foo = ConfigFactory.parseResources(getClass().getClassLoader(), "datagen.conf");
    Config dataGenConfig = foo.getConfig("source.dataGen");
    DataGeneratorConfig config = ConfigBeanFactory.create(dataGenConfig, DataGeneratorConfig.class);
    DataGenerator dataGenerator = new DataGenerator(config);
    Object schema = dataGenerator.getSchema();
    System.out.println("Schema:" + schema.toString());
    for (int i=0; i< 100; ++i) {
      Object record = dataGenerator.next();
      System.out.println("Record " + i + ":" + record.toString());
    }
  }
}
