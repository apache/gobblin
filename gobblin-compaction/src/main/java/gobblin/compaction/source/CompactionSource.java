package gobblin.compaction.source;


import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;

import com.google.common.collect.Lists;
import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.suite.CompactionAvroSuite;
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
import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.executors.IteratorExecutor;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

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

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    throw new UnsupportedOperationException("Please use getWorkunitStream");
  }

  @Override
  public WorkUnitStream getWorkunitStream(SourceState state) {
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
      CompactionWorkUnitIterator workUnitIterator = new CompactionWorkUnitIterator (verifiers);

      // Spawn a single thread to create work units
      new Thread(new SingleWorkUnitGeneratorService (datasets, workUnitIterator)).start();
      return new BasicWorkUnitStream.Builder (workUnitIterator).build();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A work unit generator will do blew steps:
   * 1) Convert dataset iterator to callable iterator, each callable is a verification procedure
   * 2) Use {@link IteratorExecutor} to execute callable iterator
   * 3) Examine the failed datasets from 2), keep retrying until timeout
   */
  private class SingleWorkUnitGeneratorService implements Runnable {
    private List<Dataset> datasets;
    private CompactionWorkUnitIterator workUnitIterator;
    private IteratorExecutor executor;

    public SingleWorkUnitGeneratorService (List<Dataset> datasets, CompactionWorkUnitIterator workUnitIterator) {
      this.datasets = datasets;
      this.workUnitIterator = workUnitIterator;
    }

    public void run () {
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();

        while (datasets.size() > 0) {
          Iterator<Callable<VerifiedDataset>> verifierIterator =
                  Iterators.transform (datasets.iterator(), new Function<Dataset, Callable<VerifiedDataset>>() {
                    @Nullable
                    @Override
                    public Callable<VerifiedDataset> apply(Dataset dataset) {
                      return new DatasetVerifier (dataset, workUnitIterator);
                    }
                  });

          executor = new IteratorExecutor<>(verifierIterator, 5,
                  ExecutorsUtils.newDaemonThreadFactory(Optional.of(log), Optional.of("Verifier-compaction-dataset-pool-%d")));

          List<Dataset> failedDatasets = Lists.newArrayList();

          List<Either<VerifiedDataset, DatasetVerificationException>> futures = executor.executeAndGetResults();
          for (Either<VerifiedDataset, DatasetVerificationException> either: futures) {
            if (either instanceof Either.Right) {
              DatasetVerificationException exc = ((Either.Right<VerifiedDataset, DatasetVerificationException>) either).getRight();
              log.error ("Dataset {} verification has exception {}", exc.dataset.datasetURN(), exc.cause);
              failedDatasets.add(exc.dataset);
            } else {
              VerifiedDataset vd = ((Either.Left<VerifiedDataset, DatasetVerificationException>) either).getLeft();
              if (!vd.isVerified) {
                log.error ("Dataset {} verification has failed", vd.dataset.datasetURN());
                failedDatasets.add(vd.dataset);
              }
            }
          }

          this.datasets = failedDatasets;
          if (stopwatch.elapsed(TimeUnit.MINUTES) > 1) {
            break;
          }
        }

        if (this.datasets.size() > 0) {
          for (Dataset dataset: datasets) {
            log.info ("{} is timed out and give up the verification, adding a failed task", dataset.datasetURN());
            // create failed task for these failed datasets
            this.workUnitIterator.addWorkUnit (createWorkUnitForFailure(dataset));
          }
        }

        this.workUnitIterator.done();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @AllArgsConstructor
  private class DatasetVerificationException extends Exception {
    private Dataset dataset;
    private Throwable cause;

    public String toString () {
      return "Exception :" + cause + " from " + dataset.datasetURN();
    }
  }

  @AllArgsConstructor
  private class VerifiedDataset {
    private Dataset dataset;
    private boolean isVerified;
  }

  /**
   * A callable responsible for dataset verification
   */
  private class DatasetVerifier implements Callable {
    private CompactionWorkUnitIterator workUnitIterator;
    private Dataset dataset;

    public DatasetVerifier (Dataset dataset, CompactionWorkUnitIterator workUnitIterator) {
      this.workUnitIterator = workUnitIterator;
      this.dataset = dataset;
    }

    /**
     * Return a wrapped dataset because we will add them back to next run if verification failed
     */
    public VerifiedDataset call () throws DatasetVerificationException {
      try {
        boolean verified = this.workUnitIterator.verify(dataset);
        if (verified) {
          this.workUnitIterator.addWorkUnit (createWorkUnit(dataset));
        }
        return new VerifiedDataset(dataset, verified);
      } catch (Exception e) {
        throw new DatasetVerificationException(dataset, e);
      }
    }
  }

  /**
   * Iterator that provides {@link WorkUnit}s for all verified {@link Dataset}s
   */
  private class CompactionWorkUnitIterator implements Iterator<WorkUnit> {
    private LinkedBlockingDeque<WorkUnit> workUnits;

    private List<CompactionVerifier> verifiers;
    private WorkUnit last;
    private AtomicBoolean isDone;

    /**
     * Constructor
     */
    public CompactionWorkUnitIterator (List<CompactionVerifier> verifiers) {
      this.workUnits = new LinkedBlockingDeque<>();
      this.verifiers = verifiers;
      this.isDone = new AtomicBoolean(false);
      this.last = null;
    }

    /**
     * Check if any {@link WorkUnit} is available. The producer is {@link SingleWorkUnitGeneratorService}
     * @return true when a new {@link WorkUnit}  is available
     *         false when producer exits
     */
    public boolean hasNext () {
      try {
        while (true) {
          if (last != null) return true;
          if (this.isDone.get() && this.workUnits.isEmpty()) return false;
          this.last = this.workUnits.poll(1, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        log.error(e.toString());
        return false;
      }
    }

    /**
     * Stops the iteration so that hasNext() returns false
     */
    public void done () {
      this.isDone.set(true);
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
          return tmp;
        } else {
          throw new IllegalStateException("last variable cannot be empty");
        }
      }

      throw new NoSuchElementException("work units queue has been exhausted");
    }

    public void remove() {
      throw new UnsupportedOperationException("No remove supported on " + this.getClass().getName());
    }

    public boolean verify (Dataset dataset) throws IOException {
      boolean verificationPassed = true;
      if (verifiers != null) {
        for (CompactionVerifier verifier : verifiers) {
          if (!verifier.verify(dataset)) {
            verificationPassed = false;
            break;
          }
        }
      }

      return verificationPassed;
    }

    protected void addWorkUnit (WorkUnit wu) {
      this.workUnits.add(wu);
    }
  }

  protected WorkUnit createWorkUnit (Dataset dataset) throws IOException {
    WorkUnit workUnit = new WorkUnit();
    TaskUtils.setTaskFactoryClass(workUnit, MRCompactionTaskFactory.class);
    suite.save (dataset, workUnit);
    return workUnit;
  }

  protected WorkUnit createWorkUnitForFailure (Dataset dataset) throws IOException {
    WorkUnit workUnit = createWorkUnit(dataset);
    workUnit.setProp(CompactionAvroSuite.MR_TASK_NEED_FAILURE, "true");
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
