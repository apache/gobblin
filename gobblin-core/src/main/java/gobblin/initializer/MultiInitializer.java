package gobblin.initializer;

import java.io.IOException;
import java.util.List;

import lombok.ToString;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;


/**
 * Wraps multiple writer initializer behind its interface. This is useful when there're more than one branch.
 */
@ToString
public class MultiInitializer implements Initializer {
  private final List<Initializer> initializers;
  private final Closer closer;

  public MultiInitializer(List<? extends Initializer> initializers) {
    this.initializers = ImmutableList.copyOf(initializers);
    this.closer = Closer.create();
    for (Initializer initializer : this.initializers) {
      this.closer.register(initializer);
    }
  }

  @Override
  public void initialize() {
    for (Initializer initializer : this.initializers) {
      initializer.initialize();
    }
  }

  @Override
  public void close() {
    try {
      this.closer.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
