package gobblin.writer.initializer;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;

public class MultiWriterInitializer implements WriterInitializer {

  private final List<WriterInitializer> writerInitializers;
  private final Closer closer;

  public MultiWriterInitializer(List<WriterInitializer> writerInitializers) {
    this.writerInitializers = Lists.newArrayList(writerInitializers);
    this.closer = Closer.create();
    for (WriterInitializer wi : this.writerInitializers) {
      closer.register(wi);
    }
  }

  @Override
  public void initialize() {
    for (WriterInitializer wi : writerInitializers) {
      wi.initialize();
    }
  }

  @Override
  public void close() {
    try {
      closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return String.format("MultiWriterInitializer [writerInitializers=%s]", writerInitializers);
  }
}
