package gobblin.compaction.suite;

import gobblin.compaction.action.CompactionCompleteAction;
import gobblin.compaction.parser.CompactionParser;
import gobblin.dataset.Dataset;
import gobblin.dataset.DatasetsFinder;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import gobblin.compaction.verify.CompactionVerifier;
import gobblin.configuration.State;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

/**
 * This class provides major components required by {@link gobblin.compaction.source.CompactionSource}
 * and {@link gobblin.compaction.mapreduce.MRCompactionTask} during the compaction.
 *
 * Some major components are:
 *
 * {@link CompactionParser}, {@link CompactionVerifier} and {@link CompactionCompleteAction}.
 *
 * The class also handles how to create a map-reduce job and how to serialized and deserialize a {@link Dataset}
 * to and from a {@link gobblin.source.workunit.WorkUnit} properly.
 */
@Slf4j
@AllArgsConstructor
public abstract class CompactionSuite<D extends Dataset> {

  /**
   * A job level state
   */
  protected final State state;

  /**
   * Provides a parser which has parsing capability to a given dataset
   */
  public abstract CompactionParser<D> getParser ();

  /**
   * Deserialize and create a new dataset from existing state
   */
  public abstract D load (State state);

  /**
   * Serialize an existing dataset to a state
   */
  public abstract void save (D dataset, State state);

  /**
   * Get a list of verifiers for dataset validation. Verifiers are executed for each dataset returned by DatasetFinder
   */
  public abstract List<CompactionVerifier<D>> getDatasetsFinderVerifiers();

  /**
   * Get a list of verifiers for dataset validation. Verifiers are executed for each dataset returned by DatasetFinder
   */
  public abstract List<CompactionVerifier<D>> getMapReduceVerifiers();

  /**
   * Map-reduce job creation
   */
  public abstract Job createJob(D dataset) throws IOException;

  /**
   * Get a list of completion actions after compaction is finished. Actions are listed in order
   */
  public abstract List<CompactionCompleteAction<D>> getCompactionCompleteActions();

}
