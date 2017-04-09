package gobblin.compaction.suite;

import gobblin.compaction.action.CompactionCompleteAction;
import gobblin.compaction.action.CompactionCompleteFileOperationAction;
import gobblin.compaction.mapreduce.CompactionAvroJobConfigurator;
import gobblin.compaction.parser.CompactionPathParser;
import gobblin.compaction.dataset.CompactionFileSystemGlobFinder;
import gobblin.compaction.verify.CompactionAuditCountVerifier;
import gobblin.compaction.verify.CompactionThresholdVerifier;
import gobblin.compaction.verify.CompactionTimeRangeVerifier;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.dataset.DatasetsFinder;
import gobblin.dataset.FileSystemDataset;
import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * A type of {@link CompactionSuite} which implements all components needed for avro file compaction.
 */
@Slf4j
public class CompactionAvroSuite extends CompactionSuite<FileSystemDataset> {

  public static final String SERIALIZE_COMPACTION_FILE_PATH_NAME = "compaction-file-path-name";

  private CompactionAvroJobConfigurator configurator = null;

  /**
   * Constructor
   */
  public CompactionAvroSuite(State state) throws IOException {
    super(state);
  }

  /**
   * Implementation of {@link CompactionSuite#getDatasetFinder()}
   * @return A {@link CompactionFileSystemGlobFinder} instance which finds all paths based a glob pattern
   */
  public DatasetsFinder getDatasetFinder () {
    try {
      Configuration conf = HadoopUtils.getConfFromState(state);
      String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(uri), conf);
      return new CompactionFileSystemGlobFinder(fs, state);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Implementation of {@link CompactionSuite#getDatasetFinder()}
   * @return A {@link CompactionPathParser} instance which parses a given {@link FileSystemDataset}
   *         to a {@link gobblin.compaction.parser.CompactionPathParser.CompactionParserResult}
   */
  public CompactionPathParser getParser () {
    return new CompactionPathParser(state);
  }

  /**
   * Implementation of {@link CompactionSuite#getDatasetsFinderVerifiers()}
   * @return A list of {@link CompactionVerifier} instances which will be verified after
   *         {@link FileSystemDataset} is found but before a {@link gobblin.source.workunit.WorkUnit}
   *         is created.
   */
  public List<CompactionVerifier<FileSystemDataset>> getDatasetsFinderVerifiers() {
    List<CompactionVerifier<FileSystemDataset>> list = new LinkedList<>();
    list.add(new CompactionTimeRangeVerifier(state));
    list.add(new CompactionThresholdVerifier(state));
    return list;
  }

  /**
   * Implementation of {@link CompactionSuite#getMapReduceVerifiers()}
   * @return A list of {@link CompactionVerifier} instances which will be verified before
   *         {@link gobblin.compaction.mapreduce.MRCompactionTask} starts the map-reduce job
   */
  public List<CompactionVerifier<FileSystemDataset>> getMapReduceVerifiers() {
    List<CompactionVerifier<FileSystemDataset>> list = new ArrayList<>();
    list.add(new CompactionAuditCountVerifier(state));
    return list;
  }

  /**
   * Serialize a dataset {@link FileSystemDataset} to a {@link State}
   * @param dataset A dataset needs serialization
   * @param state   A state that is used to save {@param dataset}
   */
  public void save (FileSystemDataset dataset, State state) {
    state.setProp(SERIALIZE_COMPACTION_FILE_PATH_NAME, dataset.datasetURN());
  }

  /**
   * Deserialize a new {@link FileSystemDataset} from a given {@link State}
   *
   * @param state a type of {@link gobblin.runtime.TaskState}
   * @return A new instance of {@link FileSystemDataset}
   */
  public FileSystemDataset load (final State state) {
    return new FileSystemDataset() {
      @Override
      public Path datasetRoot() {
        return new Path(state.getProp(SERIALIZE_COMPACTION_FILE_PATH_NAME));
      }

      @Override
      public String datasetURN() {
        return state.getProp(SERIALIZE_COMPACTION_FILE_PATH_NAME);
      }
    };
  }

  /**
   * Some post actions are required after compaction job (map-reduce) is finished.
   *
   * @return  A list of {@link CompactionCompleteAction}s which needs to be executed after
   *          map-reduce is done.
   */
  public List<CompactionCompleteAction<FileSystemDataset>> getCompactionCompleteActions() {
    ArrayList<CompactionCompleteAction<FileSystemDataset>> array = new ArrayList<>();
    array.add(new CompactionCompleteFileOperationAction(state, configurator));
    return array;
  }

  /**
   * Constructs a map-reduce job suitable for avro compaction. The detailed configuration
   * work is delegated to {@link CompactionAvroJobConfigurator#createJob(FileSystemDataset)}
   *
   * @param  dataset a top level input path which contains all avro files those need to be compacted
   * @return a map-reduce job which will compact avro files against {@param dataset}
   */
  public Job createJob (FileSystemDataset dataset) throws IOException {
    configurator = new CompactionAvroJobConfigurator(this.state);
    return configurator.createJob(dataset);
  }
}
