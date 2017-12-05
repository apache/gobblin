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

package org.apache.gobblin.publisher;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

import org.apache.gobblin.config.ConfigBuilder;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metadata.MetadataMerger;
import org.apache.gobblin.metadata.types.StaticStringMetadataMerger;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.util.FileListUtils;
import org.apache.gobblin.util.ForkOperatorUtils;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.ParallelRunner;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.WriterUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.apache.gobblin.writer.FsDataWriter;
import org.apache.gobblin.writer.FsWriterMetrics;
import org.apache.gobblin.writer.PartitionIdentifier;

import static org.apache.gobblin.util.retry.RetryerFactory.*;

/**
 * A basic implementation of {@link SingleTaskDataPublisher} that publishes the data from the writer output directory
 * to the final output directory.
 *
 * <p>
 * The final output directory is specified by {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR}. The output of each
 * writer is written to this directory. Each individual writer can also specify a path in the config key
 * {@link ConfigurationKeys#WRITER_FILE_PATH}. Then the final output data for a writer will be
 * {@link ConfigurationKeys#DATA_PUBLISHER_FINAL_DIR}/{@link ConfigurationKeys#WRITER_FILE_PATH}. If the
 * {@link ConfigurationKeys#WRITER_FILE_PATH} is not specified, a default one is assigned. The default path is
 * constructed in the {@link org.apache.gobblin.source.workunit.Extract#getOutputFilePath()} method.
 * </p>
 *
 * <p>
 * This publisher records all dirs it publishes to in property {@link ConfigurationKeys#PUBLISHER_DIRS}. Each time it
 * publishes a {@link Path}, if the path is a directory, it records this path. If the path is a file, it records the
 * parent directory of the path. To change this behavior one may override
 * {@link #recordPublisherOutputDirs(Path, Path, int)}.
 * </p>
 */
public class BaseDataPublisher extends SingleTaskDataPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(BaseDataPublisher.class);

  protected final int numBranches;
  protected final List<FileSystem> writerFileSystemByBranches;
  protected final List<FileSystem> publisherFileSystemByBranches;
  protected final List<FileSystem> metaDataWriterFileSystemByBranches;
  protected final List<Optional<String>> publisherFinalDirOwnerGroupsByBranches;
  protected final List<FsPermission> permissions;
  protected final Closer closer;
  protected final Closer parallelRunnerCloser;
  protected final int parallelRunnerThreads;
  protected final Map<String, ParallelRunner> parallelRunners = Maps.newHashMap();
  protected final Set<Path> publisherOutputDirs = Sets.newHashSet();
  protected final Optional<LineageInfo> lineageInfo;

  /* Each partition in each branch may have separate metadata. The metadata mergers are responsible
   * for aggregating this information from all workunits so it can be published.
   */
  protected final Map<PartitionIdentifier, MetadataMerger<String>> metadataMergers;
  protected final boolean shouldRetry;

  static final String DATA_PUBLISHER_RETRY_PREFIX = ConfigurationKeys.DATA_PUBLISHER_PREFIX + ".retry.";
  static final String PUBLISH_RETRY_ENABLED = DATA_PUBLISHER_RETRY_PREFIX + "enabled";

  static final Config PUBLISH_RETRY_DEFAULTS;
  protected final Config retrierConfig;

  static {
    Map<String, Object> configMap =
        ImmutableMap.<String, Object>builder()
            .put(RETRY_TIME_OUT_MS, TimeUnit.MINUTES.toMillis(2L))   //Overall retry for 2 minutes
            .put(RETRY_INTERVAL_MS, TimeUnit.SECONDS.toMillis(5L)) //Try to retry 5 seconds
            .put(RETRY_MULTIPLIER, 2L) // Muliply by 2 every attempt
            .put(RETRY_TYPE, RetryType.EXPONENTIAL.name())
            .build();
    PUBLISH_RETRY_DEFAULTS = ConfigFactory.parseMap(configMap);
  };

  public BaseDataPublisher(State state)
      throws IOException {
    super(state);
    this.closer = Closer.create();
    Configuration conf = new Configuration();

    // Add all job configuration properties so they are picked up by Hadoop
    for (String key : this.getState().getPropertyNames()) {
      conf.set(key, this.getState().getProp(key));
    }

    // Extract LineageInfo from state
    if (state instanceof SourceState) {
      lineageInfo = LineageInfo.getLineageInfo(((SourceState) state).getBroker());
    } else if (state instanceof WorkUnitState) {
      lineageInfo = LineageInfo.getLineageInfo(((WorkUnitState) state).getTaskBrokerNullable());
    } else {
      lineageInfo = Optional.absent();
    }

    this.numBranches = this.getState().getPropAsInt(ConfigurationKeys.FORK_BRANCHES_KEY, 1);
    this.shouldRetry = this.getState().getPropAsBoolean(PUBLISH_RETRY_ENABLED, false);

    this.writerFileSystemByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.publisherFileSystemByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.metaDataWriterFileSystemByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.publisherFinalDirOwnerGroupsByBranches = Lists.newArrayListWithCapacity(this.numBranches);
    this.permissions = Lists.newArrayListWithCapacity(this.numBranches);
    this.metadataMergers = new HashMap<>();

    // Get a FileSystem instance for each branch
    for (int i = 0; i < this.numBranches; i++) {
      URI writerUri = URI.create(this.getState().getProp(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, this.numBranches, i),
          ConfigurationKeys.LOCAL_FS_URI));
      this.writerFileSystemByBranches.add(FileSystem.get(writerUri, conf));

      URI publisherUri = URI.create(this.getState().getProp(ForkOperatorUtils
              .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FILE_SYSTEM_URI, this.numBranches, i),
          writerUri.toString()));
      this.publisherFileSystemByBranches.add(FileSystem.get(publisherUri, conf));
      this.metaDataWriterFileSystemByBranches.add(FileSystem.get(publisherUri, conf));

      // The group(s) will be applied to the final publisher output directory(ies)
      this.publisherFinalDirOwnerGroupsByBranches.add(Optional.fromNullable(this.getState().getProp(ForkOperatorUtils
          .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR_GROUP, this.numBranches, i))));

      // The permission(s) will be applied to all directories created by the publisher,
      // which do NOT include directories created by the writer and moved by the publisher.
      // The permissions of those directories are controlled by writer.file.permissions and writer.dir.permissions.
      this.permissions.add(new FsPermission(state.getPropAsShortWithRadix(
          ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_PERMISSIONS, this.numBranches, i),
          FsPermission.getDefault().toShort(), ConfigurationKeys.PERMISSION_PARSING_RADIX)));
    }

    if (this.shouldRetry) {
      this.retrierConfig = ConfigBuilder.create()
          .loadProps(this.getState().getProperties(), DATA_PUBLISHER_RETRY_PREFIX)
          .build()
          .withFallback(PUBLISH_RETRY_DEFAULTS);
      LOG.info("Retry enabled for publish with config : "+ retrierConfig.root().render(ConfigRenderOptions.concise()));

    }else {
      LOG.info("Retry disabled for publish.");
      this.retrierConfig = WriterUtils.NO_RETRY_CONFIG;
    }


    this.parallelRunnerThreads =
        state.getPropAsInt(ParallelRunner.PARALLEL_RUNNER_THREADS_KEY, ParallelRunner.DEFAULT_PARALLEL_RUNNER_THREADS);
    this.parallelRunnerCloser = Closer.create();
  }

  private MetadataMerger<String> buildMetadataMergerForBranch(String metadataFromConfig, int branchId,
      Path existingMetadataPath) {
    // Legacy behavior -- if we shouldn't publish writer state, instantiate a static metadata merger
    // that just returns the metadata from config (if any)
    if (!shouldPublishWriterMetadataForBranch(branchId)) {
      return new StaticStringMetadataMerger(metadataFromConfig);
    }

    String keyName = ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_MERGER_NAME_KEY, this.numBranches,
            branchId);
    String className =
        this.getState().getProp(keyName, ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_MERGER_NAME_DEFAULT);

    try {
      Class<?> mdClass = Class.forName(className);

      // If the merger understands properties, use that constructor; otherwise use the default
      // parameter-less ctor
      @SuppressWarnings("unchecked")
      Object merger = GobblinConstructorUtils
          .invokeFirstConstructor(mdClass, Collections.<Object>singletonList(this.getState().getProperties()),
              Collections.<Object>emptyList());

      try {
        @SuppressWarnings("unchecked")
        MetadataMerger<String> casted = (MetadataMerger<String>) merger;

        // Merge existing metadata from the partition if it exists..
        String existingMetadata = loadExistingMetadata(existingMetadataPath, branchId);
        if (existingMetadata != null) {
          casted.update(existingMetadata);
        }

        // Then metadata from the config...
        if (metadataFromConfig != null) {
          casted.update(metadataFromConfig);
        }
        return casted;
      } catch (ClassCastException e) {
        throw new IllegalArgumentException(className + " does not implement the MetadataMerger interface", e);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Specified metadata merger class " + className + " not found!", e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Error building merger class " + className, e);
    }
  }

  /**
   * Read in existing metadata as a UTF8 string.
   */
  private String loadExistingMetadata(Path metadataFilename, int branchId) {
    try {
      FileSystem fsForBranch = writerFileSystemByBranches.get(branchId);
      if (!fsForBranch.exists(metadataFilename)) {
        return null;
      }
      FSDataInputStream existingMetadata = writerFileSystemByBranches.get(branchId).open(metadataFilename);
      return IOUtils.toString(existingMetadata, StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.warn("IOException {} while trying to read existing metadata {} - treating as null", e.getMessage(),
          metadataFilename.toString());
      return null;
    }
  }

  @Override
  public void initialize()
      throws IOException {
    // Nothing needs to be done since the constructor already initializes the publisher.
  }

  @Override
  public void close()
      throws IOException {
    try {
      for (Path path : this.publisherOutputDirs) {
        this.state.appendToSetProp(ConfigurationKeys.PUBLISHER_DIRS, path.toString());
      }
    } finally {
      this.closer.close();
    }
  }

  private void addLineageInfo(WorkUnitState state, int branchId) {
    DatasetDescriptor destination = createDestinationDescriptor(state, branchId);
    if (this.lineageInfo.isPresent()) {
      this.lineageInfo.get().putDestination(destination, branchId, state);
    }
  }

  protected DatasetDescriptor createDestinationDescriptor(WorkUnitState state, int branchId) {
    Path publisherOutputDir = getPublisherOutputDir(state, branchId);
    FileSystem fs = this.publisherFileSystemByBranches.get(branchId);
    DatasetDescriptor destination = new DatasetDescriptor(fs.getScheme(), publisherOutputDir.toString());
    destination.addMetadata(DatasetConstants.FS_URI, fs.getUri().toString());
    destination.addMetadata(DatasetConstants.BRANCH, String.valueOf(branchId));
    return destination;
  }

  @Override
  public void publishData(WorkUnitState state)
      throws IOException {
    for (int branchId = 0; branchId < this.numBranches; branchId++) {
      publishSingleTaskData(state, branchId);
    }
    this.parallelRunnerCloser.close();
  }

  /**
   * This method publishes output data for a single task based on the given {@link WorkUnitState}.
   * Output data from other tasks won't be published even if they are in the same folder.
   */
  private void publishSingleTaskData(WorkUnitState state, int branchId)
      throws IOException {
    publishData(state, branchId, true, new HashSet<Path>());
    addLineageInfo(state, branchId);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states)
      throws IOException {

    // We need a Set to collect unique writer output paths as multiple tasks may belong to the same extract. Tasks that
    // belong to the same Extract will by default have the same output directory
    Set<Path> writerOutputPathsMoved = Sets.newHashSet();

    for (WorkUnitState workUnitState : states) {
      for (int branchId = 0; branchId < this.numBranches; branchId++) {
        publishMultiTaskData(workUnitState, branchId, writerOutputPathsMoved);
      }
    }

    this.parallelRunnerCloser.close();

    for (WorkUnitState workUnitState : states) {
      // Upon successfully committing the data to the final output directory, set states
      // of successful tasks to COMMITTED. leaving states of unsuccessful ones unchanged.
      // This makes sense to the COMMIT_ON_PARTIAL_SUCCESS policy.
      workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    }
  }

  /**
   * This method publishes task output data for the given {@link WorkUnitState}, but if there are output data of
   * other tasks in the same folder, it may also publish those data.
   */
  private void publishMultiTaskData(WorkUnitState state, int branchId, Set<Path> writerOutputPathsMoved)
      throws IOException {
    publishData(state, branchId, false, writerOutputPathsMoved);
    addLineageInfo(state, branchId);
  }

  protected void publishData(WorkUnitState state, int branchId, boolean publishSingleTaskData,
      Set<Path> writerOutputPathsMoved)
      throws IOException {
    // Get a ParallelRunner instance for moving files in parallel
    ParallelRunner parallelRunner = this.getParallelRunner(this.writerFileSystemByBranches.get(branchId));

    // The directory where the workUnitState wrote its output data.
    Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);

    if (!this.writerFileSystemByBranches.get(branchId).exists(writerOutputDir)) {
      LOG.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, state.getId()));
      return;
    }

    // The directory where the final output directory for this job will be placed.
    // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
    Path publisherOutputDir = getPublisherOutputDir(state, branchId);

    if (publishSingleTaskData) {
      // Create final output directory
      WriterUtils.mkdirsWithRecursivePermissionWithRetry(this.publisherFileSystemByBranches.get(branchId), publisherOutputDir,
          this.permissions.get(branchId), retrierConfig);
      addSingleTaskWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, state, branchId, parallelRunner);
    } else {
      if (writerOutputPathsMoved.contains(writerOutputDir)) {
        // This writer output path has already been moved for another task of the same extract
        // If publishSingleTaskData=true, writerOutputPathMoved is ignored.
        return;
      }

      if (this.publisherFileSystemByBranches.get(branchId).exists(publisherOutputDir)) {
        // The final output directory already exists, check if the job is configured to replace it.
        // If publishSingleTaskData=true, final output directory is never replaced.
        boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, branchId));

        // If the final output directory is not configured to be replaced, put new data to the existing directory.
        if (!replaceFinalOutputDir) {
          addWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, state, branchId, parallelRunner);
          writerOutputPathsMoved.add(writerOutputDir);
          return;
        }

        // Delete the final output directory if it is configured to be replaced
        LOG.info("Deleting publisher output dir " + publisherOutputDir);
        this.publisherFileSystemByBranches.get(branchId).delete(publisherOutputDir, true);
      } else {
        // Create the parent directory of the final output directory if it does not exist
        WriterUtils.mkdirsWithRecursivePermissionWithRetry(this.publisherFileSystemByBranches.get(branchId),
            publisherOutputDir.getParent(), this.permissions.get(branchId), retrierConfig);
      }

      movePath(parallelRunner, state, writerOutputDir, publisherOutputDir, branchId);
      writerOutputPathsMoved.add(writerOutputDir);
    }
  }

  /**
   * Get the output directory path this {@link BaseDataPublisher} will write to.
   *
   * <p>
   *   This is the default implementation. Subclasses of {@link BaseDataPublisher} may override this
   *   to write to a custom directory or write using a custom directory structure or naming pattern.
   * </p>
   *
   * @param workUnitState a {@link WorkUnitState} object
   * @param branchId the fork branch ID
   * @return the output directory path this {@link BaseDataPublisher} will write to
   */
  protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    return WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId);
  }

  protected void addSingleTaskWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner)
      throws IOException {
    String outputFilePropName = ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.WRITER_FINAL_OUTPUT_FILE_PATHS, this.numBranches, branchId);

    if (!workUnitState.contains(outputFilePropName)) {
      LOG.warn("Missing property " + outputFilePropName + ". This task may have pulled no data.");
      return;
    }

    Iterable<String> taskOutputFiles = workUnitState.getPropAsSet(outputFilePropName);
    for (String taskOutputFile : taskOutputFiles) {
      Path taskOutputPath = new Path(taskOutputFile);
      if (!this.writerFileSystemByBranches.get(branchId).exists(taskOutputPath)) {
        LOG.warn("Task output file " + taskOutputFile + " doesn't exist.");
        continue;
      }
      String pathSuffix = taskOutputFile
          .substring(taskOutputFile.indexOf(writerOutputDir.toString()) + writerOutputDir.toString().length() + 1);
      Path publisherOutputPath = new Path(publisherOutputDir, pathSuffix);
      WriterUtils.mkdirsWithRecursivePermissionWithRetry(this.publisherFileSystemByBranches.get(branchId),
          publisherOutputPath.getParent(), this.permissions.get(branchId), retrierConfig);

      movePath(parallelRunner, workUnitState, taskOutputPath, publisherOutputPath, branchId);
    }
  }

  protected void addWriterOutputToExistingDir(Path writerOutputDir, Path publisherOutputDir,
      WorkUnitState workUnitState, int branchId, ParallelRunner parallelRunner)
      throws IOException {
    boolean preserveFileName = workUnitState.getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, this.numBranches, branchId),
        false);
    // Go through each file in writerOutputDir and move it into publisherOutputDir
    for (FileStatus status : this.writerFileSystemByBranches.get(branchId).listStatus(writerOutputDir)) {

      // Preserve the file name if configured, use specified name otherwise
      Path finalOutputPath = preserveFileName ? new Path(publisherOutputDir, workUnitState.getProp(ForkOperatorUtils
          .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_FINAL_NAME, this.numBranches, branchId)))
          : new Path(publisherOutputDir, status.getPath().getName());

      movePath(parallelRunner, workUnitState, status.getPath(), finalOutputPath, branchId);
    }
  }

  protected void movePath(ParallelRunner parallelRunner, State state, Path src, Path dst, int branchId)
      throws IOException {
    LOG.info(String.format("Moving %s to %s", src, dst));
    boolean overwrite = state.getPropAsBoolean(ConfigurationKeys.DATA_PUBLISHER_OVERWRITE_ENABLED, false);
    this.publisherOutputDirs.addAll(recordPublisherOutputDirs(src, dst, branchId));
    parallelRunner.movePath(src, this.publisherFileSystemByBranches.get(branchId), dst, overwrite,
        this.publisherFinalDirOwnerGroupsByBranches.get(branchId));
  }

  protected Collection<Path> recordPublisherOutputDirs(Path src, Path dst, int branchId)
      throws IOException {

    // Getting file status from src rather than dst, because at this time dst doesn't yet exist.
    // If src is a dir, add dst to the set of paths. Otherwise, add dst's parent.
    if (this.writerFileSystemByBranches.get(branchId).getFileStatus(src).isDirectory()) {
      return ImmutableList.<Path>of(dst);
    }
    return ImmutableList.<Path>of(dst.getParent());
  }

  private ParallelRunner getParallelRunner(FileSystem fs) {
    String uri = fs.getUri().toString();
    if (!this.parallelRunners.containsKey(uri)) {
      this.parallelRunners
          .put(uri, this.parallelRunnerCloser.register(new ParallelRunner(this.parallelRunnerThreads, fs)));
    }
    return this.parallelRunners.get(uri);
  }

  /**
   * Merge all of the metadata output from each work-unit and publish the merged record.
   * @param states States from all tasks
   * @throws IOException If there is an error publishing the file
   */
  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states)
      throws IOException {
   Set<String> partitions = new HashSet<>();

    // There should be one merged metadata file per branch; first merge all of the pieces together
    mergeMetadataAndCollectPartitionNames(states, partitions);
    partitions.removeIf(Objects::isNull);

    // Now, pick an arbitrary WorkUnitState to get config information around metadata such as
    // the desired output filename. We assume that publisher config settings
    // are the same across all workunits so it doesn't really matter which workUnit we retrieve this information
    // from.

    WorkUnitState anyState = states.iterator().next();
    for (int branchId = 0; branchId < numBranches; branchId++) {
      String mdOutputPath = getMetadataOutputPathFromState(anyState, branchId);
      String userSpecifiedPath = getUserSpecifiedOutputPathFromState(anyState, branchId);

      if (partitions.isEmpty() || userSpecifiedPath != null) {
        publishMetadata(getMergedMetadataForPartitionAndBranch(null, branchId),
            branchId,
            getMetadataOutputFileForBranch(anyState, branchId));
      } else {
        String metadataFilename = getMetadataFileNameForBranch(anyState, branchId);
        if (mdOutputPath == null || metadataFilename == null) {
          LOG.info("Metadata filename not set for branch " + String.valueOf(branchId) + ": not publishing metadata.");
          continue;
        }

        for (String partition : partitions) {
          publishMetadata(getMergedMetadataForPartitionAndBranch(partition, branchId),
              branchId,
              new Path(new Path(mdOutputPath, partition), metadataFilename));
        }
      }
    }
  }

  /*
   * Metadata that we publish can come from several places:
   *  - It can be passed in job config (DATA_PUBLISHER_METADATA_STR)
   *  - It can be picked up from previous runs of a job (if the output partition already exists)
   *  -- The above two are handled when we construct a new MetadataMerger
   *
   *  - The source/converters/writers associated with each branch of a job may add their own metadata
   *    (eg: this dataset is encrypted using AES256). This is returned by getIntermediateMetadataFromState()
   *    and fed into the MetadataMerger.
   *  - FsWriterMetrics can be emitted and rolled up into metadata. These metrics are specific to a {partition, branch}
   *    combo as they mention per-output file metrics. This is also fed into metadata mergers.
   *
   *  Each writer should only be a part of one branch, but it may be responsible for multiple partitions.
   */
  private void mergeMetadataAndCollectPartitionNames(Collection<? extends WorkUnitState> states,
      Set<String> partitionPaths) {

    for (WorkUnitState workUnitState : states) {
      // First extract the partition paths and metrics from the work unit. This is essentially
      // equivalent to grouping FsWriterMetrics by {partitionKey, branchId} and extracting
      // all partitionPaths into a set.
      Map<PartitionIdentifier, Set<FsWriterMetrics>> metricsByPartition = new HashMap<>();
      boolean partitionFound = false;
      for (Map.Entry<Object, Object> property : workUnitState.getProperties().entrySet()) {
        if (((String) property.getKey()).startsWith(ConfigurationKeys.WRITER_PARTITION_PATH_KEY)) {
          partitionPaths.add((String) property.getValue());
          partitionFound = true;
        } else if (((String) property.getKey()).startsWith(FsDataWriter.FS_WRITER_METRICS_KEY)) {
          try {
            FsWriterMetrics parsedMetrics = FsWriterMetrics.fromJson((String) property.getValue());
            partitionPaths.add(parsedMetrics.getPartitionInfo().getPartitionKey());
            Set<FsWriterMetrics> metricsForPartition =
                metricsByPartition.computeIfAbsent(parsedMetrics.getPartitionInfo(), k -> new HashSet<>());
            metricsForPartition.add(parsedMetrics);
          } catch (IOException e) {
            LOG.warn("Error parsing metrics from property {} - ignoring", (String) property.getValue());
          }
        }
      }

      // no specific partitions - add null as a placeholder
      if (!partitionFound) {
        partitionPaths.add(null);
      }

      final String configBasedMetadata = getMetadataFromWorkUnitState(workUnitState);

      // Now update all metadata mergers with branch metadata + partition metrics
      for (int branchId = 0; branchId < numBranches; branchId++) {
        for (String partition : partitionPaths) {
          PartitionIdentifier partitionIdentifier = new PartitionIdentifier(partition, branchId);
          final int branch = branchId;
          MetadataMerger<String> mdMerger = metadataMergers.computeIfAbsent(partitionIdentifier,
              k -> buildMetadataMergerForBranch(configBasedMetadata, branch,
                  getMetadataOutputFileForBranch(workUnitState, branch)));
          if (shouldPublishWriterMetadataForBranch(branchId)) {
            String md = getIntermediateMetadataFromState(workUnitState, branchId);
            mdMerger.update(md);
            Set<FsWriterMetrics> metricsForPartition =
                metricsByPartition.getOrDefault(partitionIdentifier, Collections.emptySet());
            for (FsWriterMetrics metrics : metricsForPartition) {
              mdMerger.update(metrics);
            }
          }
        }
      }
    }
  }


  /**
   * Publish metadata for each branch. We expect the metadata to be of String format and
   * populated in either the WRITER_MERGED_METADATA_KEY state or the WRITER_METADATA_KEY configuration key.
   */
  @Override
  public void publishMetadata(WorkUnitState state)
      throws IOException {
    publishMetadata(Collections.singleton(state));
  }

  /**
   * Publish metadata to a set of paths
   */
  private void publishMetadata(String metadataValue, int branchId, Path metadataOutputPath)
      throws IOException {
    try {
      if (metadataOutputPath == null) {
        LOG.info("Metadata output path not set for branch " + String.valueOf(branchId) + ", not publishing.");
        return;
      }

      if (metadataValue == null) {
        LOG.info("No metadata collected for branch " + String.valueOf(branchId) + ", not publishing.");
        return;
      }

      FileSystem fs = this.metaDataWriterFileSystemByBranches.get(branchId);

        if (!fs.exists(metadataOutputPath.getParent())) {
          WriterUtils.mkdirsWithRecursivePermissionWithRetry(fs, metadataOutputPath, this.permissions.get(branchId), retrierConfig);
        }

      //Delete the file if metadata already exists
      if (fs.exists(metadataOutputPath)) {
        HadoopUtils.deletePath(fs, metadataOutputPath, false);
      }
      LOG.info("Writing metadata for branch " + String.valueOf(branchId) + " to " + metadataOutputPath.toString());
      try (FSDataOutputStream outputStream = fs.create(metadataOutputPath)) {
        outputStream.write(metadataValue.getBytes(StandardCharsets.UTF_8));
      }
    } catch (IOException e) {
      LOG.error("Metadata file is not generated: " + e, e);
    }
  }

  private String getMetadataFileNameForBranch(WorkUnitState state, int branchId) {
    // Note: This doesn't follow the pattern elsewhere in Gobblin where we have branch specific config
    // parameters! Leaving this way for backwards compatibility.
    String filePrefix = state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_FILE);
    return ForkOperatorUtils.getPropertyNameForBranch(filePrefix, this.numBranches, branchId);
  }

  private Path getMetadataOutputFileForBranch(WorkUnitState state, int branchId) {
    String metaDataOutputDirStr = getMetadataOutputPathFromState(state, branchId);
    String fileName = getMetadataFileNameForBranch(state, branchId);
    if (metaDataOutputDirStr == null || fileName == null) {
      return null;
    }
    return new Path(metaDataOutputDirStr, fileName);
  }

  private String getUserSpecifiedOutputPathFromState(WorkUnitState state, int branchId) {
    String outputDir = state.getProp(ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR, this.numBranches, branchId));

    // An older version of this code did not get a branch specific PUBLISHER_METADATA_OUTPUT_DIR so fallback
    // for compatibility's sake
    if (outputDir == null && this.numBranches > 1) {
      outputDir = state.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR);
      if (outputDir != null) {
        LOG.warn("Branches are configured for this job but a per branch metadata output directory was not set;"
            + " is this intended?");
      }
    }

    return outputDir;
  }

  private String getMetadataOutputPathFromState(WorkUnitState state, int branchId) {
    String outputDir = getUserSpecifiedOutputPathFromState(state, branchId);

    // Just write out to the regular output path if a metadata specific path hasn't been provided
    if (outputDir == null) {
      String publisherOutputDir = getPublisherOutputDir(state, branchId).toString();
      LOG.info("Missing metadata output directory path : " + ConfigurationKeys.DATA_PUBLISHER_METADATA_OUTPUT_DIR
          + " in the config; assuming outputPath " + publisherOutputDir);
      return publisherOutputDir;
    }

    return outputDir;
  }

  /*
   * Retrieve intermediate metadata (eg the metadata stored by each writer) for a given state and branch id.
   */
  private String getIntermediateMetadataFromState(WorkUnitState state, int branchId) {
    return state.getProp(
        ForkOperatorUtils.getPropertyNameForBranch(ConfigurationKeys.WRITER_METADATA_KEY, this.numBranches, branchId));
  }

  /*
   * Get the merged metadata given a workunit state and branch id. This method assumes
   * all intermediate metadata has already been passed to the MetadataMerger.
   *
   * If metadata mergers are not configured, instead return the metadata from job config that was
   * passed in by the user.
   */
  private String getMergedMetadataForPartitionAndBranch(String partitionId, int branchId) {
    String mergedMd = null;
    MetadataMerger<String> mergerForBranch = metadataMergers.get(new PartitionIdentifier(partitionId, branchId));
    if (mergerForBranch != null) {
      mergedMd = mergerForBranch.getMergedMetadata();
      if (mergedMd == null) {
        LOG.warn("Metadata merger for branch {} returned null - bug in merger?", branchId);
      }
    }

    return mergedMd;
  }

  private boolean shouldPublishWriterMetadataForBranch(int branchId) {
    String keyName = ForkOperatorUtils
        .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISH_WRITER_METADATA_KEY, this.numBranches, branchId);
    return this.getState().getPropAsBoolean(keyName, false);
  }

  /**
   * Retrieve metadata from job state config
   */
  private String getMetadataFromWorkUnitState(WorkUnitState workUnitState) {
    return workUnitState.getProp(ConfigurationKeys.DATA_PUBLISHER_METADATA_STR);
  }

  /**
   * The BaseDataPublisher relies on publishData() to create and clean-up the output directories, so data
   * has to be published before the metadata can be.
   */
  @Override
  protected boolean shouldPublishMetadataFirst() {
    return false;
  }
}
