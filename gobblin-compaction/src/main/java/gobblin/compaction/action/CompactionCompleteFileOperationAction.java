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
   * Replace or append the destination folder with new avro files from map-reduce job
   * Create a record count file containing the number of records that have been processed .
   */
  public void onCompactionJobComplete (FileSystemDataset dataset) throws IOException {
    if (configurator != null && configurator.isJobCreated()) {
      CompactionPathParser.CompactionParserResult result = new CompactionPathParser(state).parse(dataset);
      Path tmpPath = configurator.getMrOutputPath();
      Path dstPath = new Path (result.getDstAbsoluteDir());

      // this is append delta mode due to the compaction rename source dir mode being enabled
      boolean appendDeltaOutput = this.state.getPropAsBoolean(MRCompactor.COMPACTION_RENAME_SOURCE_DIR_ENABLED,
              MRCompactor.DEFAULT_COMPACTION_RENAME_SOURCE_DIR_ENABLED);

      // Obtain record count from input file names
      // We are not getting record count from map-reduce counter because in next run, the threshold (delta record)
      // calculation is based on the input file names.
      long newTotalRecords = 0;
      long oldTotalRecords = InputRecordCountHelper.readRecordCount (helper.getFs(), new Path (result.getDstAbsoluteDir()));
      if (this.state.contains(InputRecordCountHelper.INPUT_TOTAL_RECORD_COUNT)) {
        newTotalRecords = this.state.getPropAsLong(InputRecordCountHelper.INPUT_TOTAL_RECORD_COUNT);
      } else {
        log.warn ("Dataset {} doesn't have input total record count, this may be because user skipped threshold verification", dataset.datasetURN());
      }

      if (appendDeltaOutput) {
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
      InputRecordCountHelper.writeRecordCount (helper.getFs(), new Path (result.getDstAbsoluteDir()), newTotalRecords);
      log.info("Updating record count from {} to {} in {} ", oldTotalRecords, newTotalRecords, dstPath);
    }
  }

  public String getName () {
    return CompactionCompleteFileOperationAction.class.getName();
  }
}
