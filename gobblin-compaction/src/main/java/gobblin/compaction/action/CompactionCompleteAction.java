package gobblin.compaction.action;
import gobblin.configuration.State;
import gobblin.dataset.Dataset;
import lombok.AllArgsConstructor;


/**
 * An interface which represents an action that is invoked after a compaction job is finished.
 */
public interface CompactionCompleteAction<D extends Dataset> {
  void onCompactionJobCompelete(D dataset);
}
