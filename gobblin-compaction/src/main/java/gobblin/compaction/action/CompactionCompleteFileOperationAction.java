package gobblin.compaction.action;

import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * A type of post action {@link CompactionCompleteAction} which focus on the file operations
 */
@Slf4j
@AllArgsConstructor
public class CompactionCompleteFileOperationAction implements CompactionCompleteAction<FileSystemDataset> {

  protected State state;
  private CompactionAvroJobConfigurator configurator;

  /**
   * Perform some file operations like move, rename, overwrite, delete, etc.
   * @return status object or null depends on user implementation
   */
  public void onCompactionJobCompelete (FileSystemDataset dataset) {
    if (configurator != null && configurator.isJobCreated()) {

      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      log.info ("Moving {} from {}/{} to {}/{}", result.getDatasetName(), result.getSrcBaseDir(), result.getSrcSubDir(),
              result.getDstBaseDir(), result.getDstSubDir());
    }
  }
}
