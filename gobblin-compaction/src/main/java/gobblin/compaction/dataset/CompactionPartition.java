package gobblin.compaction.dataset;

import gobblin.configuration.State;
import gobblin.dataset.Dataset;

import lombok.Getter;

import org.apache.hadoop.fs.Path;


/**
 * A class represents a single path which contains partition information.
 * The path should reflects the fine grained partition size user wants to compact, like daily, hourly, minutely, etc.
 * A {@link CompactionSuite} is provided to validate ({@link gobblin.compaction.verify.CompactionVerifier}) or
 * parse ({@link CompactionParser}) the current {@link CompactionPartition} when it is required.
 */
public class CompactionPartition implements Dataset {
  public static final String COMPACTION_PARTITION_PATH = "compaction-partition-path";

  @Getter
  private final Path path;
  @Getter
  private final CompactionSuite suite;

  public CompactionPartition (Path path, CompactionSuite suite) {
    this.path = path;
    this.suite = suite;
  }

  public CompactionPartition(State state) {
    this.path = new Path (state.getProp(COMPACTION_PARTITION_PATH));
    this.suite = new CompactionSuite(state);
  }

  public void save (State state) {
    state.setProp(COMPACTION_PARTITION_PATH, this.path.toString());
  }

  public String datasetURN() {
    return this.suite.getParser().parse(this).getDatasetName();
  }

}
