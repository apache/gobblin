/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gobblin.compaction.source;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import gobblin.compaction.mapreduce.MRCompactor;
import gobblin.compaction.suite.CompactionSuiteUtils;
import gobblin.config.ConfigBuilder;
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
import gobblin.runtime.task.FailedTask;
import gobblin.runtime.task.TaskUtils;
import gobblin.source.Source;
import gobblin.source.WorkUnitStreamSource;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.BasicWorkUnitStream;
import gobblin.source.workunit.WorkUnit;
import gobblin.source.workunit.WorkUnitStream;
import gobblin.util.ClassAliasResolver;
import gobblin.util.Either;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;
import gobblin.util.executors.IteratorExecutor;
import gobblin.util.reflection.GobblinConstructorUtils;
import gobblin.util.request_allocation.GreedyAllocator;
import gobblin.util.request_allocation.HierarchicalAllocator;
import gobblin.util.request_allocation.HierarchicalPrioritizer;
import gobblin.util.request_allocation.RequestAllocator;
import gobblin.util.request_allocation.RequestAllocatorConfig;
import gobblin.util.request_allocation.RequestAllocatorUtils;
import gobblin.data.management.dataset.SimpleDatasetRequest;
import gobblin.data.management.dataset.SimpleDatasetRequestor;
import gobblin.util.request_allocation.ResourceEstimator;
import gobblin.util.request_allocation.ResourcePool;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
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
  private RequestAllocator<SimpleDatasetRequest> allocator;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    throw new UnsupportedOperationException("Please use getWorkunitStream");
  }

  @Override
  public WorkUnitStream getWorkunitStream(SourceState state) {
    try {
      fs = getSourceFileSystem(state);
      suite = CompactionSuiteUtils.getCompactionSuiteFactory(state).createSuite(state);

      initRequestAllocator(state);
      initJobDir(state);
      copyJarDependencies(state);
      DatasetsFinder finder = DatasetUtils.instantiateDatasetFinder(state.getProperties(),
              getSourceFileSystem(state),
              DefaultFileSystemGlobFinder.class.getName());

      List<Dataset> datasets = finder.findDatasets();
      CompactionWorkUnitIterator workUnitIterator = new CompactionWorkUnitIterator ();

      // Spawn a single thread to create work units
      new Thread(new SingleWorkUnitGeneratorService (state, prioritize(datasets, state), workUnitIterator), "SingleWorkUnitGeneratorService").start();
      return new BasicWorkUnitStream.Builder (workUnitIterator).build();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A work unit generator service will do the following:
   * 1) Convert dataset iterator to verification callable iterator, each callable element is a verification procedure
   * 2) Use {@link IteratorExecutor} to execute callable iterator
   * 3) Collect all failed datasets at step 2), retry them until timeout. Once timeout create failed workunits on purpose.
   */
  private class SingleWorkUnitGeneratorService implements Runnable {
    private SourceState state;
    private List<Dataset> datasets;
    private CompactionWorkUnitIterator workUnitIterator;
    private IteratorExecutor executor;

    public SingleWorkUnitGeneratorService (SourceState state, List<Dataset> datasets, CompactionWorkUnitIterator workUnitIterator) {
      this.state = state;
      this.datasets = datasets;
      this.workUnitIterator = workUnitIterator;
    }

    public void run () {
      try {
        Stopwatch stopwatch = Stopwatch.createStarted();
        int threads = this.state.getPropAsInt(CompactionVerifier.COMPACTION_VERIFICATION_THREADS, 5);
        long timeOutInMinute = this.state.getPropAsLong(CompactionVerifier.COMPACTION_VERIFICATION_TIMEOUT_MINUTES, 30);
        long iterationCountLimit = this.state.getPropAsLong(CompactionVerifier.COMPACTION_VERIFICATION_ITERATION_COUNT_LIMIT, Integer.MAX_VALUE);
        long iteration = 0;
        while (datasets.size() > 0 && iteration++ < iterationCountLimit) {
          Iterator<Callable<VerifiedDataset>> verifierIterator =
                  Iterators.transform (datasets.iterator(), new Function<Dataset, Callable<VerifiedDataset>>() {
                    @Override
                    public Callable<VerifiedDataset> apply(Dataset dataset) {
                      return new DatasetVerifier (dataset, workUnitIterator, suite.getDatasetsFinderVerifiers());
                    }
                  });

          executor = new IteratorExecutor<>(verifierIterator, threads,
                  ExecutorsUtils.newThreadFactory(Optional.of(log), Optional.of("Verifier-compaction-dataset-pool-%d")));

          List<Dataset> failedDatasets = Lists.newArrayList();

          List<Either<VerifiedDataset, ExecutionException>> futures = executor.executeAndGetResults();
          for (Either<VerifiedDataset, ExecutionException> either: futures) {
            if (either instanceof Either.Right) {
              ExecutionException exc = ((Either.Right<VerifiedDataset, ExecutionException>) either).getRight();
              DatasetVerificationException dve = (DatasetVerificationException) exc.getCause();
              log.error ("Verification raised an exception:" + ExceptionUtils.getStackTrace(dve.cause));
              failedDatasets.add(dve.dataset);
            } else {
              VerifiedDataset vd = ((Either.Left<VerifiedDataset, ExecutionException>) either).getLeft();
              if (!vd.verifiedResult.allVerificationPassed) {
                if (vd.verifiedResult.shouldRetry) {
                  log.error ("Dataset {} verification has failure but should retry", vd.dataset.datasetURN());
                  failedDatasets.add(vd.dataset);
                } else {
                  log.error ("Dataset {} verification has failure but no need to retry", vd.dataset.datasetURN());
                }
              }
            }
          }

          this.datasets = prioritize(failedDatasets, state);
          if (stopwatch.elapsed(TimeUnit.MINUTES) > timeOutInMinute) {
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
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }
  }

  private void initRequestAllocator (State state) {
    try {
      ResourceEstimator estimator = GobblinConstructorUtils.<ResourceEstimator>invokeLongestConstructor(
          new ClassAliasResolver(ResourceEstimator.class).resolveClass(state.getProp(ConfigurationKeys.COMPACTION_ESTIMATOR,
              SimpleDatasetRequest.SimpleDatasetCountEstimator.class.getName())));

      RequestAllocatorConfig.Builder<SimpleDatasetRequest> configBuilder =
          RequestAllocatorConfig.builder(estimator).allowParallelization(1).withLimitedScopeConfig(ConfigBuilder.create()
              .loadProps(state.getProperties(), ConfigurationKeys.COMPACTION_PRIORITIZATION_PREFIX).build());

      if (!state.contains(ConfigurationKeys.COMPACTION_PRIORITIZER_ALIAS)) {
        allocator = new GreedyAllocator<>(configBuilder.build());
        return;
      }

      Comparator<SimpleDatasetRequest> prioritizer = GobblinConstructorUtils.<Comparator>invokeLongestConstructor(
          new ClassAliasResolver(Comparator.class).resolveClass(state.getProp(ConfigurationKeys.COMPACTION_PRIORITIZER_ALIAS)), state);

      configBuilder.withPrioritizer(prioritizer);

      if (prioritizer instanceof HierarchicalPrioritizer) {
        allocator = new HierarchicalAllocator.Factory().createRequestAllocator(configBuilder.build());
      } else {
        allocator = RequestAllocatorUtils.inferFromConfig(configBuilder.build());
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Cannot initialize allocator", e);
    }
  }

  private List<Dataset> prioritize (List<Dataset> datasets, State state) {
    double maxPool = state.getPropAsDouble(MRCompactor.COMPACTION_DATASETS_MAX_COUNT, MRCompactor.DEFUALT_COMPACTION_DATASETS_MAX_COUNT);
    ResourcePool pool = ResourcePool.builder().maxResource(SimpleDatasetRequest.SIMPLE_DATASET_COUNT_DIMENSION, maxPool).build();

    Iterator<Dataset> newList = Iterators.transform(
        this.allocator.allocateRequests(datasets.stream().map(SimpleDatasetRequestor::new).iterator(), pool), (input) -> input.getDataset());
    return Lists.newArrayList(newList);
  }

  private static class DatasetVerificationException extends Exception {
    private Dataset dataset;
    private Throwable cause;

    public DatasetVerificationException (Dataset dataset, Throwable cause) {
      super ("Dataset:" + dataset.datasetURN() + " Exception:" + cause);
      this.dataset = dataset;
      this.cause = cause;
    }
  }

  @AllArgsConstructor
  private static class VerifiedDataset {
    private Dataset dataset;
    private VerifiedResult verifiedResult;
  }

  @AllArgsConstructor
  private static class VerifiedResult {
    private boolean allVerificationPassed;
    private boolean shouldRetry;
  }

  @AllArgsConstructor
  private class DatasetVerifier implements Callable {
    private Dataset dataset;
    private CompactionWorkUnitIterator workUnitIterator;
    private List<CompactionVerifier> verifiers;

    /**
     * {@link VerifiedDataset} wraps original {@link Dataset} because if verification failed, we are able get original
     * datasets and restart the entire process of verification against those failed datasets.
     */
    public VerifiedDataset call () throws DatasetVerificationException {
      try {
        VerifiedResult result = this.verify(dataset);
        if (result.allVerificationPassed) {
          this.workUnitIterator.addWorkUnit (createWorkUnit(dataset));
        }
        return new VerifiedDataset(dataset, result);
      } catch (Exception e) {
        throw new DatasetVerificationException(dataset, e);
      }
    }

    public VerifiedResult verify (Dataset dataset) throws Exception {
      boolean verificationPassed = true;
      boolean shouldRetry = true;
      if (verifiers != null) {
        for (CompactionVerifier verifier : verifiers) {
          if (!verifier.verify (dataset)) {
            verificationPassed = false;
            // Not all verification should be retried. Below are verifications which
            // doesn't need retry. If any of then failed, we simply skip this dataset.
            if (!verifier.isRetriable()) {
              shouldRetry = false;
              break;
            }
          }
        }
      }

      return new VerifiedResult(verificationPassed, shouldRetry);
    }
  }

  /**
   * Iterator that provides {@link WorkUnit}s for all verified {@link Dataset}s
   */
  private static class CompactionWorkUnitIterator implements Iterator<WorkUnit> {
    private LinkedBlockingDeque<WorkUnit> workUnits;
    private WorkUnit last;
    private AtomicBoolean isDone;

    /**
     * Constructor
     */
    public CompactionWorkUnitIterator () {
      this.workUnits = new LinkedBlockingDeque<>();
      this.isDone = new AtomicBoolean(false);
      this.last = null;
    }

    /**
     * Check if any {@link WorkUnit} is available. The producer is {@link SingleWorkUnitGeneratorService}
     * @return true when a new {@link WorkUnit} is available
     *         false when {@link CompactionWorkUnitIterator#isDone} is invoked
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
     * Stops the iteration so that {@link CompactionWorkUnitIterator#hasNext()} returns false
     */
    public void done () {
      this.isDone.set(true);
    }

    /**
     * Obtain next available {@link WorkUnit}.
     * The method will first query if any work unit is available by calling {@link CompactionWorkUnitIterator#hasNext()}
     * Because {@link CompactionWorkUnitIterator#hasNext()} is a blocking call, this method can also be blocked.
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
    WorkUnit workUnit = new FailedTask.FailedWorkUnit();
    TaskUtils.setTaskFactoryClass(workUnit, CompactionFailedTask.CompactionFailedTaskFactory.class);
    suite.save (dataset, workUnit);
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
