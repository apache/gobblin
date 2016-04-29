package gobblin.converter.initializer;

import java.util.List;

import lombok.ToString;
import gobblin.initializer.Initializer;
import gobblin.initializer.MultiInitializer;

@ToString
public class MultiConverterInitializer implements ConverterInitializer {
  private final Initializer intializer;

  public MultiConverterInitializer(List<ConverterInitializer> converterInitializers) {
    intializer = new MultiInitializer(converterInitializers);
  }

  @Override
  public void initialize() {
    intializer.initialize();
  }

  @Override
  public void close() {
    intializer.close();
  }
}
