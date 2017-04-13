package gobblin.compaction.source;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import gobblin.data.management.dataset.DatasetUtils;
import gobblin.data.management.dataset.DefaultFileSystemGlobFinder;
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
import gobblin.util.ClassAliasResolver;
import gobblin.util.HadoopUtils;
import gobblin.util.reflection.GobblinConstructorUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * A compaction source derived from {@link Source} which uses {@link DefaultFileSystemGlobFinder} to find all
 * {@link Dataset}s. Use {@link CompactionSuite#getDatasetsFinderVerifiers()} to guarantee a given dataset has passed
 * all verification. Each found dataset will be serialized to {@link WorkUnit} by {@link CompactionSuite#save(Dataset, State)}
 */
@Slf4j
public class CompactionSource implements Source<String, String> {
  private CompactionSuite suite;
  private ClassAliasResolver<CompactionSuite> suiteAliasResolver = new ClassAliasResolver<>(CompactionSuite.class);

  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    String suiteName = state.getProp(ConfigurationKeys.COMPACTION_SUITE_NAME, ConfigurationKeys.DEFAULT_COMPACTION_SUITE_NAME);

    try {
      suite = GobblinConstructorUtils.invokeLongestConstructor(
              suiteAliasResolver.resolveClass(suiteName), ImmutableList.of(state).toArray());

      DatasetsFinder finder  = DatasetUtils.instantiateDatasetFinder(state.getProperties(),
               getSourceFileSystem(state),
               DefaultFileSystemGlobFinder.class.getName());

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
    } catch (Exception e) {
      throw new RuntimeException(e);
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

  protected FileSystem getSourceFileSystem(State state)
          throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }

}
