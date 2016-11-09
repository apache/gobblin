package gobblin.initializer;

import lombok.ToString;

@ToString
public class NoopInitializer implements Initializer {
  @Override
  public void initialize() {}

  @Override
  public void close() {}
}
