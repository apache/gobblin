package gobblin.compaction.suite;


import gobblin.configuration.State;

/**
 * Build {@link CompactionSuite} for a job execution
 */
public interface CompactionSuiteFactory {
  CompactionSuite createSuite (State state);
}
