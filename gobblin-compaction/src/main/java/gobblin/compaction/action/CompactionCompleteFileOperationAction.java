package gobblin.compaction.action;

import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.mapreduce.MRCompactorJobRunner;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.compaction.verify.InputRecordCountHelper;
import gobblin.configuration.State;
import gobblin.dataset.FileSystemDataset;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.IOException;


/**
 * A type of post action {@link CompactionCompleteAction} which focus on the file operations
 */
@Slf4j
@AllArgsConstructor
public class CompactionCompleteFileOperationAction implements CompactionCompleteAction<FileSystemDataset> {

  protected State state;
  private CompactionAvroJobConfigurator configurator;
  private InputRecordCountHelper helper;
  private FileSystem fs;

  public CompactionCompleteFileOperationAction (State state, CompactionAvroJobConfigurator configurator) {
    this.state = state;
    this.helper = new InputRecordCountHelper(state);
    this.configurator = configurator;
    this.fs = configurator.getFs();
  }

  /**
   * Replace the destination folder with new output from map-reduce job
   * and create a file for next run record count comparison.
   */
  public void onCompactionJobComplete (FileSystemDataset dataset) {
    if (configurator != null && configurator.isJobCreated()) {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      Path tmpPath = configurator.getMrOutputPath();
      Path dstPath = new Path (result.getDstAbsoluteDir());

      try {
        // move output from mapreduce to final destination defined by dataset
        this.fs.delete(dstPath, true);
        FsPermission permission = HadoopUtils.deserializeFsPermission(this.state,
                                                                      MRCompactorJobRunner.COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
                                                                      FsPermission.getDefault());

        WriterUtils.mkdirsWithRecursivePermission (this.fs, dstPath.getParent(), permission);
        if (!this.fs.rename(tmpPath, dstPath)) {
          throw new IOException(
                  String.format("Unable to move %s to %s", tmpPath, dstPath));
        }

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
