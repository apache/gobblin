package gobblin.mapreduce;

import gobblin.util.recordcount.CompactionRecordCountProvider;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * An {@link FileOutputCommitter} that commits json files specified
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 *
 * Created by tamasnemeth on 13/12/16.
 */
public class JsonCompactorOutputCommitter extends FileOutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(JsonCompactorOutputCommitter.class);

  public JsonCompactorOutputCommitter(Path output, TaskAttemptContext context) throws IOException {
    super(output, context);
  }

  /**
   * Commits the task, moving files to their final committed location by delegating to
   * {@link FileOutputCommitter} to perform the actual moving. First, renames the
   * files to include the count of records contained within the file and a timestamp,
   * in the form {recordCount}.{timestamp}.extension. Then, the files are moved to their
   * committed location.
   */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    Path workPath = getWorkPath();
    FileSystem fs = workPath.getFileSystem(context.getConfiguration());

    if (fs.exists(workPath)) {
      long recordCount = getRecordCountFromCounter(context, JsonKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT);
      String fileNamePrefix;
      if (recordCount == 0) {

        // recordCount == 0 indicates that it is a map-only, non-dedup job, and thus record count should
        // be obtained from mapper counter.
        fileNamePrefix = CompactionRecordCountProvider.M_OUTPUT_FILE_PREFIX;
        recordCount = getRecordCountFromCounter(context, JsonKeyDedupReducer.EVENT_COUNTER.RECORD_COUNT);
      } else {
        fileNamePrefix = CompactionRecordCountProvider.MR_OUTPUT_FILE_PREFIX;
      }

      String fileName = CompactionRecordCountProvider.constructFileName(fileNamePrefix, ".gz", recordCount);

      for (FileStatus status : fs.listStatus(workPath, new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return FilenameUtils.isExtension(path.getName(), "gz");
        }
      })) {
        Path newPath = new Path(status.getPath().getParent(), fileName);
        LOG.info(String.format("Renaming %s to %s", status.getPath(), newPath));
        fs.rename(status.getPath(), newPath);
      }
    }

    super.commitTask(context);
  }

  private long getRecordCountFromCounter(TaskAttemptContext context, Enum<?> counterName) {
    try {
      Method getCounterMethod = context.getClass().getMethod("getCounter", Enum.class);
      return ((Counter) getCounterMethod.invoke(context, counterName)).getValue();
    } catch (Exception e) {
      throw new RuntimeException("Error reading record count counter", e);
    }

  }


}
