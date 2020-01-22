package org.apache.gobblin.test.generator.avro;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.ValueGeneratorFactory;
import org.apache.gobblin.test.generator.ValueGeneratorFactoryWithFallback;
import org.apache.gobblin.test.generator.java.JavaValueGeneratorFactory;


public class AvroValueGeneratorFactory extends ValueGeneratorFactoryWithFallback {
  protected AvroValueGeneratorFactory() {
    super(InMemoryFormat.AVRO_GENERIC, JavaValueGeneratorFactory.getInstance());
  }

  private static AvroValueGeneratorFactory INSTANCE = new AvroValueGeneratorFactory();

  public static ValueGeneratorFactory getInstance() {
    return INSTANCE;
  }
}
