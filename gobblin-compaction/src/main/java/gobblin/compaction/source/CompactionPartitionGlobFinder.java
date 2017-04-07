package gobblin.compaction.source;


import gobblin.compaction.dataset.CompactionPartition;
import gobblin.compaction.dataset.CompactionSuite;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.data.management.retention.profile.ConfigurableGlobDatasetFinder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

/**
 * A subclass of {@link ConfigurableGlobDatasetFinder} which find all the {@link CompactionPartition} units to
 * participate the compaction process.
 */
public class CompactionPartitionGlobFinder extends ConfigurableGlobDatasetFinder<CompactionPartition> {
  State state;
  public CompactionPartitionGlobFinder(FileSystem fs, SourceState state) throws IOException {
    super(fs, state.getProperties());
    this.state = state;
  }

  public CompactionPartition datasetAtPath(Path path) throws IOException {
    return new CompactionPartition(path, new CompactionSuite(state));
  }
}
