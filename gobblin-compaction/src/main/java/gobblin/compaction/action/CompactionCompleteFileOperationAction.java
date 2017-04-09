package gobblin.compaction.action;

import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.extern.slf4j.Slf4j;


/**
 * A type of post action {@link CompactionCompleteAction} which focus on the file operations
 */
@Slf4j
public class CompactionCompleteFileOperationAction extends CompactionCompleteAction<FileSystemDataset>{
  private CompactionAvroJobConfigurator configurator;
  public CompactionCompleteFileOperationAction(State state, CompactionAvroJobConfigurator configurator) {
    super(state);
    this.configurator = configurator;
  }

  /**
   * Perform some file operations like move, rename, overwrite, delete, etc.
   * @return status object or null depends on user implementation
   */
  public Object onCompactionJobCompelete (FileSystemDataset dataset) {
    if (configurator != null && configurator.isJobCreated()) {
      //TODO: we can move, overwrite, rename input or output directory.
      log.info ("Moving, overwriting, renaming input or output folder");
    }
    return null;
  }
}
