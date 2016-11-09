package gobblin.data.management.partition;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import gobblin.data.management.copy.CopyEntity;
import gobblin.dataset.Dataset;


/**
 * A non-lazy {@link FileSet} where the copy entities are a static, eagerly computed list of {@link CopyEntity}s.
 * @param <T>
 */
public class StaticFileSet<T extends CopyEntity> extends FileSet<T> {

  private final List<T> files;

  public StaticFileSet(String name, Dataset dataset, List<T> files) {
    super(name, dataset);
    this.files = files;
  }

  @Override
  protected Collection<T> generateCopyEntities()
      throws IOException {
    return this.files;
  }
}
