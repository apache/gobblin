package gobblin.converter.initializer;

import lombok.ToString;
import gobblin.initializer.Initializer;
import gobblin.initializer.NoopInitializer;

@ToString
public class NoopConverterInitializer implements ConverterInitializer {
  private final Initializer initializer = new NoopInitializer();

  @Override
  public void initialize() {
    initializer.initialize();
  }

  @Override
  public void close() {
    initializer.close();
  }

}
