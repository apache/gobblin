package gobblin.data.management.partition;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import gobblin.data.management.copy.CopyConfiguration;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableDatasetBase;
import gobblin.data.management.copy.IterableCopyableDataset;
import gobblin.data.management.copy.IterableCopyableDatasetImpl;
import gobblin.data.management.copy.prioritization.PrioritizedCopyableDataset;
import gobblin.util.request_allocation.PushDownRequestor;

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;


/**
 * A wrapper around a {@link CopyableDatasetBase} that makes it a {@link PushDownRequestor} for prioritization.
 */
@AllArgsConstructor
public class CopyableDatasetRequestor implements PushDownRequestor<FileSet<CopyEntity>> {

  @AllArgsConstructor
  public static class Factory implements Function<CopyableDatasetBase, CopyableDatasetRequestor> {
    private final FileSystem targetFs;
    private final CopyConfiguration copyConfiguration;
    private final Logger log;

    @Nullable
    @Override
    public CopyableDatasetRequestor apply(CopyableDatasetBase input) {
      IterableCopyableDataset iterableCopyableDataset;
      if (input instanceof IterableCopyableDataset) {
        iterableCopyableDataset = (IterableCopyableDataset) input;
      } else if (input instanceof CopyableDataset) {
        iterableCopyableDataset = new IterableCopyableDatasetImpl((CopyableDataset) input);
      } else {
        log.error(String.format("Cannot process %s, can only copy %s or %s.",
            input == null ? null : input.getClass().getName(),
            CopyableDataset.class.getName(), IterableCopyableDataset.class.getName()));
        return null;
      }
      return new CopyableDatasetRequestor(this.targetFs, this.copyConfiguration, iterableCopyableDataset);
    }
  }

  private final FileSystem targetFs;
  private final CopyConfiguration copyConfiguration;
  private final IterableCopyableDataset dataset;

  @Override
  public Iterator<FileSet<CopyEntity>> getRequests() throws IOException {
    return injectRequestor(this.dataset.getFileSetIterator(this.targetFs, this.copyConfiguration));
  }

  @Override
  public Iterator<FileSet<CopyEntity>> getRequests(Comparator<FileSet<CopyEntity>> prioritizer) throws IOException {
    if (this.dataset instanceof PrioritizedCopyableDataset) {
      return injectRequestor(((PrioritizedCopyableDataset) this.dataset).getFileSetIterator(this.targetFs, this.copyConfiguration, prioritizer));
    }

    List<FileSet<CopyEntity>> entities = Lists.newArrayList(this.dataset.getFileSetIterator(this.targetFs, this.copyConfiguration));
    Collections.sort(entities, prioritizer);
    return injectRequestor(entities.iterator());
  }

  private Iterator<FileSet<CopyEntity>> injectRequestor(Iterator<FileSet<CopyEntity>> iterator) {
    return Iterators.transform(iterator, new Function<FileSet<CopyEntity>, FileSet<CopyEntity>>() {
      @Override
      public FileSet<CopyEntity> apply(FileSet<CopyEntity> input) {
        input.setRequestor(CopyableDatasetRequestor.this);
        return input;
      }
    });
  }
}
