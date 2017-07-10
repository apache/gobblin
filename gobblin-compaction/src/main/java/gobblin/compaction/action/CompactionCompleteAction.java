package gobblin.compaction.action;
import gobblin.dataset.Dataset;
import gobblin.metrics.event.EventSubmitter;

import java.io.IOException;


/**
 * An interface which represents an action that is invoked after a compaction job is finished.
 */
public interface CompactionCompleteAction<D extends Dataset> {

  void onCompactionJobComplete(D dataset) throws IOException;

  default void addEventSubmitter(EventSubmitter submitter) {
    throw new UnsupportedOperationException("Please add an EventSubmitter");
  }
}
