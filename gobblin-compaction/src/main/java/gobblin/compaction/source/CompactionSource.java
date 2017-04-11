package gobblin.compaction.source;

import com.google.common.collect.Lists;

import gobblin.compaction.dataset.CompactionFileSystemGlobFinder;
import gobblin.compaction.suite.CompactionAvroSuite;
import gobblin.compaction.suite.CompactionSuite;
import gobblin.compaction.verify.CompactionVerifier;
import gobblin.compaction.mapreduce.MRCompactionTaskFactory;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.dataset.Dataset;
import gobblin.dataset.DatasetsFinder;
import gobblin.runtime.task.TaskUtils;
import gobblin.source.Source;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * A compaction source derived from {@link Source} which uses {@link CompactionFileSystemGlobFinder} to find all
 * {@link Dataset}s. Use {@link CompactionSuite#getDatasetsFinderVerifiers()} to guarantee a given dataset has passed
 * all verification. Each found dataset will be serialized to {@link WorkUnit} by {@link CompactionSuite#save(Dataset, State)}
 */
public class CompactionSource implements Source<String, String> {
  CompactionSuite suite;

  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    try {
      //TODO: use reflection to get real suite
      suite = new CompactionAvroSuite(state);
      DatasetsFinder finder = getDatasetFinder(state);
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
  public Extractor getExtractor (WorkUnitState state) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown (SourceState state) {
    throw new UnsupportedOperationException();
  }

  public DatasetsFinder getDatasetFinder (SourceState state) {
    try {
      Configuration conf = HadoopUtils.getConfFromState(state);
      String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
      FileSystem fs = FileSystem.get(URI.create(uri), conf);
      return new CompactionFileSystemGlobFinder(fs, state);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
