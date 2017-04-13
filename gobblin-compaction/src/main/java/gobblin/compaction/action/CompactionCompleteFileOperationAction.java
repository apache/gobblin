package gobblin.compaction.action;

import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.compaction.verify.InputRecordCountHelper;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;


/**
 * A type of post action {@link CompactionCompleteAction} which focus on the file operations
 */
@Slf4j
@AllArgsConstructor
public class CompactionCompleteFileOperationAction implements CompactionCompleteAction<FileSystemDataset> {

  protected State state;
  private CompactionAvroJobConfigurator configurator;
  private InputRecordCountHelper helper;

  public CompactionCompleteFileOperationAction (State state, CompactionAvroJobConfigurator configurator) {
    this.state = state;
    this.helper = new InputRecordCountHelper(state);
    this.configurator = configurator;
  }

  /**
   * Perform some file operations like move, rename, overwrite, delete, etc.
   * @return status object or null depends on user implementation
   */
  public void onCompactionJobComplete (FileSystemDataset dataset) {
    if (configurator != null && configurator.isJobCreated()) {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      try {
        Path dstPath = new Path(result.getDstAbsoluteDir());

        // get the record count from directories that map-reduce has processed
        long count = helper.calculateRecordCount(configurator.getMapReduceInputPaths());
        InputRecordCountHelper.writeRecordCount (helper.getFs(), new Path (result.getDstAbsoluteDir()), count);
        log.info("Writing record count {} to {}", count, dstPath);
      } catch (Exception e) {
        log.error(e.toString());
      }
    }
  }

  public String getName () {
    return CompactionCompleteFileOperationAction.class.getName();
  }
}
