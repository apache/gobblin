package gobblin.compaction.action;

import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;

/**
 * Class responsible for hive registration after compaction is complete
 */
public class CompactionHiveRegistrationAction implements CompactionCompleteAction<FileSystemDataset> {
  State state;
  public CompactionHiveRegistrationAction (State state) {
    this.state = state;
  }

  public void onCompactionJobComplete(FileSystemDataset dataset) {

  }
}
