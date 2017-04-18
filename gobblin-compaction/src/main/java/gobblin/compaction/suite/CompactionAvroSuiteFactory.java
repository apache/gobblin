package gobblin.compaction.suite;

import gobblin.annotation.Alias;
import gobblin.configuration.State;

/**
 * A {@link CompactionSuiteFactory} that handles {@link CompactionAvroSuite} creation logic.
 */
@Alias("CompactionAvroSuiteFactory")
public class CompactionAvroSuiteFactory implements CompactionSuiteFactory {
  public CompactionAvroSuite createSuite (State state) {
    return new CompactionAvroSuite (state);
  }
}
