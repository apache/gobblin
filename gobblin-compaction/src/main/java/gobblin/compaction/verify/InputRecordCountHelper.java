package gobblin.compaction.verify;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import gobblin.compaction.dataset.DatasetHelper;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.HadoopUtils;
import gobblin.util.RecordCountProvider;
import gobblin.util.recordcount.IngestionRecordCountProvider;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;

/**
 * A class helps to calculate, serialize, deserialize record count.
 *
 * By using {@link IngestionRecordCountProvider}, the default input file name should be in format
 * {file_name}.{record_count}.{extension}. For example, given a file path: "/a/b/c/file.123.avro",
 * the record count will be 123.
 */
@Slf4j
public class InputRecordCountHelper {

  @Getter
  private final FileSystem fs;
  private final State state;
  private final RecordCountProvider inputRecordCountProvider;
  private final String AVRO = "avro";

  public final static String RECORD_COUNT_FILE = "_record_count";

  /**
   * Constructor
   */
  public InputRecordCountHelper(State state) {
    try {
      this.fs = getSourceFileSystem (state);
      this.state = state;
      this.inputRecordCountProvider = (RecordCountProvider) Class
              .forName(state.getProp(MRCompactor.COMPACTION_INPUT_RECORD_COUNT_PROVIDER,
                      MRCompactor.DEFAULT_COMPACTION_INPUT_RECORD_COUNT_PROVIDER))
              .newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to instantiate " + InputRecordCountHelper.class.getName(), e);
    }
  }

  /**
   * Calculate record count at given paths
   * @param  paths all paths where the record count are calculated
   * @return record count after parsing all files under given paths
   */
  public long calculateRecordCount (Collection<Path> paths) throws IOException {
    long sum = 0;
    for (Path path: paths) {
      sum += inputRecordCountProvider.getRecordCount(DatasetHelper.getApplicableFilePaths(this.fs, path, Lists.newArrayList(AVRO)));
    }
    return sum;
  }

  /**
   * Read record count from a specific directory.
   * File name is {@link InputRecordCountHelper#RECORD_COUNT_FILE}
   * @param fs  file system in use
   * @param dir directory where a record file will be read
   * @return record count
   */
  public static long readRecordCount (FileSystem fs, Path dir) throws IOException {
    if (!fs.exists(new Path(dir, RECORD_COUNT_FILE))) {
      return 0;
    }

    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open (new Path (dir, RECORD_COUNT_FILE)), Charsets.UTF_8))) {
      long count = Long.parseLong(br.readLine());
      return count;
    }
  }

  /**
   * Write record count to a specific directory.
   * File name is {@link InputRecordCountHelper#RECORD_COUNT_FILE}
   * @param fs file system in use
   * @param dir directory where a record file will be saved
   */
  public static void writeRecordCount (FileSystem fs, Path dir, long count) throws IOException {
    try (FSDataOutputStream outputFileStream = fs.create(new Path(dir, RECORD_COUNT_FILE))) {
      outputFileStream.writeBytes(Long.toString(count));
    }
  }

  protected FileSystem getSourceFileSystem (State state)
          throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }
}
