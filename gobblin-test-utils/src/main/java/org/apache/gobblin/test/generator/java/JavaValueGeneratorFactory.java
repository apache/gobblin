package org.apache.gobblin.test.generator.java;

import org.apache.gobblin.test.InMemoryFormat;
import org.apache.gobblin.test.generator.RegisteredValueGeneratorFactory;
import org.apache.gobblin.test.generator.ValueGeneratorFactory;


@RegisteredValueGeneratorFactory(inMemoryFormat= InMemoryFormat.POJO)
public class JavaValueGeneratorFactory extends ValueGeneratorFactory {
  protected JavaValueGeneratorFactory() {
    super(InMemoryFormat.POJO);
  }

  private static JavaValueGeneratorFactory INSTANCE = new JavaValueGeneratorFactory();

  public static ValueGeneratorFactory getInstance() {
    return INSTANCE;
  }
}
