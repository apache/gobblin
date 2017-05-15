package gobblin.compaction.action;

import com.google.common.collect.Lists;
import gobblin.compaction.dataset.DatasetHelper;
import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.mapreduce.MRCompactorJobRunner;
import gobblin.compaction.mapreduce.avro.AvroKeyMapper;
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
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;


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
  public void onCompactionJobComplete (FileSystemDataset dataset) throws IOException {
    if (configurator != null && configurator.isJobCreated()) {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      Path tmpPath = configurator.getMrOutputPath();
      Path dstPath = new Path (result.getDstAbsoluteDir());

      // get record count from map reduce job counter
      Job job = this.configurator.getConfiguredJob();
      Counter counter = job.getCounters().findCounter(AvroKeyMapper.EVENT_COUNTER.RECORD_COUNT);
      long recordCount = counter.getValue();


      boolean renamingRequired = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
              MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

      if (renamingRequired) {
        FsPermission permission = HadoopUtils.deserializeFsPermission(this.state,
                MRCompactorJobRunner.COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
                FsPermission.getDefault());
        WriterUtils.mkdirsWithRecursivePermission(this.fs, dstPath, permission);
        // append files under mr output to destination
        List<Path> paths = DatasetHelper.getApplicableFilePaths(fs, tmpPath, Lists.newArrayList("avro"));
        for (Path path: paths) {
          String fileName = path.getName();
          log.info(String.format("Adding %s to %s", path.toString(), dstPath));
          Path outPath = new Path (dstPath, fileName);

          if (!this.fs.rename(path, outPath)) {
            throw new IOException(
                    String.format("Unable to move %s to %s", path.toString(), outPath.toString()));
          }
        }

        // update record count (adding previous count)
        long delta = InputRecordCountHelper.readRecordCount(fs, dstPath);
        log.info("Will change record count from {} to {} at {}", recordCount, delta, dstPath);
        recordCount += delta;
        
      } else {
        this.fs.delete(dstPath, true);
        FsPermission permission = HadoopUtils.deserializeFsPermission(this.state,
                MRCompactorJobRunner.COMPACTION_JOB_OUTPUT_DIR_PERMISSION,
                FsPermission.getDefault());

        WriterUtils.mkdirsWithRecursivePermission(this.fs, dstPath.getParent(), permission);
        if (!this.fs.rename(tmpPath, dstPath)) {
          throw new IOException(
                  String.format("Unable to move %s to %s", tmpPath, dstPath));
        }
      }

      // write record count
      InputRecordCountHelper.writeRecordCount (helper.getFs(), new Path (result.getDstAbsoluteDir()), recordCount);
      log.info("Writing record count {} to {}", recordCount, dstPath);
    }
  }

  public String getName () {
    return CompactionCompleteFileOperationAction.class.getName();
  }
}
