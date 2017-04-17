package gobblin.compaction.suite;

import gobblin.compaction.action.CompactionCompleteAction;
import gobblin.compaction.mapreduce.MRCompactionTask;
import gobblin.configuration.SourceState;
import gobblin.data.management.copy.replication.ConfigBasedDatasetsFinder;
import gobblin.dataset.Dataset;

import gobblin.compaction.verify.CompactionVerifier;
import gobblin.configuration.State;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;

/**
 * This interface provides major components required by {@link gobblin.compaction.source.CompactionSource}
 * and {@link gobblin.compaction.mapreduce.MRCompactionTask} flow.
 *
 * User needs to implement {@link #createJob(Dataset)} method to create a customized map-reduce job.
 * Two types of {@link CompactionVerifier}s should be provided. One is to verify datasets returned by
 * {@link ConfigBasedDatasetsFinder#findDatasets()}. The other is to verify datasets before we run MR
 * job inside {@link gobblin.compaction.mapreduce.MRCompactionTask}
 *
 * The class also handles how to create a map-reduce job and how to serialized and deserialize a {@link Dataset}
 * to and from a {@link gobblin.source.workunit.WorkUnit} properly.
 */

public interface CompactionSuite<D extends Dataset> {

  /**
   * Deserialize and create a new dataset from existing state
   */
  D load (State state);

  /**
   * Serialize an existing dataset to a state
   */
  void save (D dataset, State state);

  /**
   * Get a list of verifiers for each dataset validation.
   * Verifiers are executed by {@link gobblin.compaction.source.CompactionSource#getWorkunits(SourceState)}
   */
  List<CompactionVerifier<D>> getDatasetsFinderVerifiers();

  /**
   * Get a list of verifiers for each dataset validation.
   * Verifiers are executed by {@link MRCompactionTask#run()}
   */
  List<CompactionVerifier<D>> getMapReduceVerifiers();

  /**
   * Map-reduce job creation
   */
  Job createJob(D dataset) throws IOException;

  /**
   * Get a list of completion actions after compaction is finished. Actions are listed in order
   */
  List<CompactionCompleteAction<D>> getCompactionCompleteActions();

}
