package org.apache.gobblin.test.generator.json;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.ValueGeneratorFactory;
import org.apache.gobblin.test.generator.ValueGeneratorFactoryWithFallback;
import org.apache.gobblin.test.generator.java.JavaValueGeneratorFactory;


public class JsonValueGeneratorFactory extends ValueGeneratorFactoryWithFallback {
  protected JsonValueGeneratorFactory() {
    super(InMemoryFormat.JSON, JavaValueGeneratorFactory.getInstance());
  }

  private static JsonValueGeneratorFactory INSTANCE = new JsonValueGeneratorFactory();

  public static ValueGeneratorFactory getInstance() {
    return INSTANCE;
  }
}
