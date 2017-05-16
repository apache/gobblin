package gobblin.compaction.action;
import gobblin.dataset.Dataset;

import java.io.IOException;


/**
 * An interface which represents an action that is invoked after a compaction job is finished.
 */
public interface CompactionCompleteAction<D extends Dataset> {
  void onCompactionJobComplete(D dataset) throws IOException;
}
