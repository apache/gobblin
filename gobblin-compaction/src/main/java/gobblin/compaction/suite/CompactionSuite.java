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
 * and {@link gobblin.compaction.mapreduce.MRCompactionTask} to use during the compaction. Some major components are:
 *
 * {@link DatasetsFinder}, {@link CompactionParser}, {@link CompactionVerifier} and {@link CompactionCompleteAction}.
 *
 * The class also handles how to create a map-reduce job and how to serialized and deserialize a {@link Dataset}
 * to a {@link State} so that we serialize and deserialized a dataset to {@link gobblin.source.workunit.WorkUnit} properly.
 */
@Slf4j
@AllArgsConstructor
public abstract class CompactionSuite<D extends Dataset> {
  protected final State state;

  public abstract DatasetsFinder<D> getDatasetFinder ();

  public abstract CompactionParser<D> getParser ();

  public abstract D load (State state);

  public abstract void save (D dataset, State state);

  public abstract List<CompactionVerifier<D>> getDatasetsFinderVerifiers();

  public abstract List<CompactionVerifier<D>> getMapReduceVerifiers();

  public abstract Job createJob(D dataset) throws IOException;

  public abstract List<CompactionCompleteAction<D>> getCompactionCompleteActions();

}
