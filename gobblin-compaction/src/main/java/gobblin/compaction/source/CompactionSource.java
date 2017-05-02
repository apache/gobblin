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
import gobblin.runtime.JobState;
import gobblin.runtime.task.TaskUtils;
import gobblin.source.Source;
import gobblin.source.WorkUnitStreamSource;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.BasicWorkUnitStream;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.WorkUnitStream;
import gobblin.util.HadoopUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.List;

/**
 * A compaction source derived from {@link Source} which uses {@link DefaultFileSystemGlobFinder} to find all
 * {@link Dataset}s. Use {@link CompactionSuite#getDatasetsFinderVerifiers()} to guarantee a given dataset has passed
 * all verification. Each found dataset will be serialized to {@link WorkUnit} by {@link CompactionSuite#save(Dataset, State)}
 */
@Slf4j
public class CompactionSource implements WorkUnitStreamSource<String, String> {
  private CompactionSuite suite;
  private Path tmpJobDir;
  private FileSystem fs;
  private ExecutorService service;

  @Override
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

  @Override
  public WorkUnitStream getWorkunitStream(SourceState state) {
    service = Executors.newSingleThreadExecutor();

    try {
      fs = getSourceFileSystem(state);
      suite = CompactionSuiteUtils.getCompactionSuiteFactory(state).createSuite(state);

      initJobDir(state);
      copyJarDependencies(state);
      List<CompactionVerifier> verifiers = suite.getDatasetsFinderVerifiers();
      DatasetsFinder finder = DatasetUtils.instantiateDatasetFinder(state.getProperties(),
              getSourceFileSystem(state),
              DefaultFileSystemGlobFinder.class.getName());

      List<Dataset> datasets = finder.findDatasets();
      CompactionDatasetIterator iterator = new CompactionDatasetIterator(datasets, verifiers);
      service.submit(iterator.getDatasetProcessor());
      return new BasicWorkUnitStream.Builder (iterator).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Iterator that provides {@link WorkUnit}s for all verified {@link Dataset}s
   */
  public class CompactionDatasetIterator implements Iterator<WorkUnit> {
    private LinkedBlockingDeque<WorkUnit> workUnits;

    private List<Dataset> datasets;
    private List<CompactionVerifier> verifiers;
    private WorkUnit last;
    private volatile int unProcessed;
    private volatile int success;

    /**
     * Constructor
     */
    public CompactionDatasetIterator (List<Dataset> datasets, List<CompactionVerifier> verifiers) {
      this.datasets = datasets;
      this.workUnits = new LinkedBlockingDeque<>();
      this.verifiers = verifiers;
      this.unProcessed = datasets.size();
      this.success = 0;
      this.last = null;
    }

    /**
     * Check if any {@link WorkUnit} is available. The producer is {@link CompactionDatasetIterator#getDatasetProcessor()}
     * @return true when a new {@link WorkUnit}  is available
     *         false when producer exits
     */
    public boolean hasNext () {
      try {
        while (true) {
          if (last != null) {
            log.debug ("hasNext() is true because cache is not empty");
            return true;
          }
          if (this.unProcessed > 0) {
            last = this.workUnits.poll(1, TimeUnit.SECONDS);
            if (last == null)
              log.debug ("Waiting for producer to complete...");
          } else {
            if (workUnits.size() > 0) {
              log.debug ("hasNext() has new element");
              return true;
            }
            log.debug ("hasNext() returns false because producer is complete");
            return false;
          }
        }
      } catch (InterruptedException e) {
        log.error(e.toString());
        return false;
      }
    }

    /**
     * Obtain next available {@link WorkUnit}.
     * Block the consumer thread until a new {@link WorkUnit} is provided. Otherwise throw an exception
     */
    public WorkUnit next () {
      if (hasNext()) {
        if (last != null) {
          WorkUnit tmp = last;
          last = null;
          log.debug ("next() pops out a workunit");
          return tmp;
        }
      }

      throw new NoSuchElementException ("work units queue has been exhausted");
    }

    public void remove() {
      throw new UnsupportedOperationException("No remove supported on " + this.getClass().getName());
    }

    public Callable<Integer> getDatasetProcessor () {
      return new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          for (Dataset dataset: datasets) {
            // all verifier should be passed before we compact the dataset
            boolean verificationPassed = true;
            if (verifiers != null) {
              for (CompactionVerifier verifier : verifiers) {
                if (!verifier.verify(dataset)) {
                  verificationPassed = false;
                  break;
                }
              }
            }
            if (verificationPassed) {
              success++;
              workUnits.add (createWorkUnit(dataset));
            }
            unProcessed--;
          }

          return success;
        }
      };
    }
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
      service.shutdown();
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
   * Create a temporary job directory based on job id or (if not available) UUID
   */
  private void initJobDir (SourceState state) throws IOException {
    String tmpBase = state.getProp(MRCompactor.COMPACTION_TMP_DEST_DIR, MRCompactor.DEFAULT_COMPACTION_TMP_DEST_DIR);
    String jobId;

    if (state instanceof JobState) {
      jobId = ((JobState) state).getJobId();
    } else {
      jobId = UUID.randomUUID().toString();
    }

    this.tmpJobDir = new Path (tmpBase, jobId);
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
