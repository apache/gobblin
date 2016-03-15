package gobblin.writer.initializer;

import java.io.Closeable;

public interface WriterInitializer extends Closeable {

  /**
   * Initialize for the writer. Note that this may modify workUnit to pass
   * initialization information to writer
   *
   * @param state
   * @param workUnits WorkUnits created by Source
   */
  public void initialize();

  /**
   * Removed checked exception.
   * {@inheritDoc}
   * @see java.io.Closeable#close()
   */
  @Override
  public void close();
}