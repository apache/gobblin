package gobblin;

import java.io.Closeable;

public interface Initializer extends Closeable {

  /**
   * Initialize for the writer.
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
