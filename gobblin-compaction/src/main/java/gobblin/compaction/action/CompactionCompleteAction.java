package gobblin.compaction.action;
import gobblin.configuration.State;
import gobblin.dataset.Dataset;
import lombok.AllArgsConstructor;


/**
 * A class which represents an action that is invoked after a compaction job is finished.
 */
@AllArgsConstructor
public abstract class CompactionCompleteAction<D extends Dataset> {
  State state;
  public abstract Object onCompactionJobCompelete(D dataset);
}
