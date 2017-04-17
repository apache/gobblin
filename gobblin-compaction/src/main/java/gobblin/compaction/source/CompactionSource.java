package gobblin.compaction.source;

import com.google.common.collect.Lists;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.suite.CompactionSuiteUtils;
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
import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;

/**
 * A compaction source derived from {@link Source} which uses {@link DefaultFileSystemGlobFinder} to find all
 * {@link Dataset}s. Use {@link CompactionSuite#getDatasetsFinderVerifiers()} to guarantee a given dataset has passed
 * all verification. Each found dataset will be serialized to {@link WorkUnit} by {@link CompactionSuite#save(Dataset, State)}
 */
@Slf4j
public class CompactionSource implements Source<String, String> {
  private CompactionSuite suite;
  private Path tmpJobDir;
  private FileSystem fs;

  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    try {
      fs = getSourceFileSystem(state);
      suite = CompactionSuiteUtils.getCompactionSuiteFactory (state).createSuite(state);

      initJobDir (state);
      copyJarDependencies (state);

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
    try {
      boolean f = fs.delete(this.tmpJobDir, true);
      log.info("Job dir is removed from {} with status {}", this.tmpJobDir, f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected FileSystem getSourceFileSystem(State state)
          throws IOException {
    Configuration conf = HadoopUtils.getConfFromState(state);
    String uri = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI);
    return HadoopUtils.getOptionallyThrottledFileSystem(FileSystem.get(URI.create(uri), conf), state);
  }

  /**
   * Create a UUID based random job directory
   */
  private void initJobDir (State state) throws IOException {
    String tmpBase = state.getProp(MRCompactor.COMPACTION_TMP_DEST_DIR, MRCompactor.DEFAULT_COMPACTION_TMP_DEST_DIR);
    this.tmpJobDir = new Path (tmpBase, UUID.randomUUID().toString());
    this.fs.mkdirs(this.tmpJobDir);
    state.setProp (MRCompactor.COMPACTION_JOB_DIR, tmpJobDir.toString());
    log.info ("Job dir is created under {}", this.tmpJobDir);
  }

  /**
   * Copy dependent jars to a temporary job directory on HDFS
   */
  private void copyJarDependencies (State state) throws IOException {
    if (this.tmpJobDir == null) {
      throw new RuntimeException("Job directory is not created");
    }

    if (!state.contains(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      return;
    }

    // create sub-dir to save jar files
    LocalFileSystem lfs = FileSystem.getLocal(HadoopUtils.getConfFromState(state));
    Path tmpJarFileDir = new Path(this.tmpJobDir, MRCompactor.COMPACTION_JAR_SUBDIR);
    this.fs.mkdirs(tmpJarFileDir);
    state.setProp (MRCompactor.COMPACTION_JARS, tmpJarFileDir.toString());

    // copy jar files to hdfs
    for (String jarFile : state.getPropAsList(ConfigurationKeys.JOB_JAR_FILES_KEY)) {
      for (FileStatus status : lfs.globStatus(new Path(jarFile))) {
        Path tmpJarFile = new Path(this.fs.makeQualified(tmpJarFileDir), status.getPath().getName());
        this.fs.copyFromLocalFile(status.getPath(), tmpJarFile);
        log.info(String.format("%s will be added to classpath", tmpJarFile));
      }
    }
  }
}
