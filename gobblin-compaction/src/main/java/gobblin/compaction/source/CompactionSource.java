package gobblin.compaction.source;

import com.google.common.collect.Lists;

import gobblin.compaction.suite.CompactionAvroSuite;
import gobblin.compaction.suite.CompactionSuite;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.compaction.mapreduce.MRCompactionTaskFactory;
import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.Dataset;
import gobblin.dataset.DatasetsFinder;
import gobblin.runtime.task.TaskUtils;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A compaction source derived from {@link Source} which uses {@link CompactionSuite#getDatasetFinder()} to find all
 * {@link Dataset}s. Use {@link CompactionSuite#getDatasetsFinderVerifiers()} to guarantee a given dataset has passed
 * all verification. The MR compaction job is created by {@link gobblin.compaction.mapreduce.configurator.CompactionJobConfigurator}.
 * After the job finishes, some post actions are performed by {@link gobblin.compaction.action.CompactionCompleteAction}.
 */
public class CompactionSource implements Source<String, String> {
  CompactionSuite suite;

  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    try {
      //TODO: use reflection to get real suite
      suite = new CompactionAvroSuite(state);
      DatasetsFinder finder = suite.getDatasetFinder();
      List<CompactionVerifier> verifiers = suite.getDatasetsFinderVerifiers();
      List<Dataset> datasets = finder.findDatasets();

      for (Dataset dataset: datasets) {
        // all verifier should be passed before we compact the dataset
        boolean verificationPassed = true;
        for (CompactionVerifier verifier : verifiers) {
          if (!verifier.verify(dataset)) {
            verificationPassed = false;
            break;
          }
        }

        if (verificationPassed) {
          WorkUnit workUnit = createWorkUnit(dataset);
          workUnits.add(workUnit);
        }
      }
    } catch (IOException e) {
      return null;
    }

    return workUnits;
  }

  protected WorkUnit createWorkUnit (Dataset dataset) throws IOException {
    WorkUnit workUnit = new WorkUnit();
    TaskUtils.setTaskFactoryClass(workUnit, MRCompactionTaskFactory.class);
    suite.save(dataset, workUnit);
    return workUnit;
  }

  @Override
  public Extractor getExtractor(WorkUnitState state) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown(SourceState state) {
    throw new UnsupportedOperationException();
  }
}
