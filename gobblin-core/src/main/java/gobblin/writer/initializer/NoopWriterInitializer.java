package gobblin.writer.initializer;

public class NoopWriterInitializer implements WriterInitializer {

  @Override
  public void close() {}

  @Override
  public void initialize() {}
}
