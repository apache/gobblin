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

package org.apache.gobblin.data.management.copy.publisher;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.data.management.copy.PreserveAttributes;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.filesystem.DataFileVersionStrategy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.commit.CommitStep;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.configuration.WorkUnitState.WorkingState;
import org.apache.gobblin.data.management.copy.CopyEntity;
import org.apache.gobblin.data.management.copy.CopySource;
import org.apache.gobblin.data.management.copy.CopyableDataset;
import org.apache.gobblin.data.management.copy.CopyableDatasetMetadata;
import org.apache.gobblin.data.management.copy.CopyableFile;
import org.apache.gobblin.data.management.copy.entities.CommitStepCopyEntity;
import org.apache.gobblin.data.management.copy.entities.PostPublishStep;
import org.apache.gobblin.data.management.copy.entities.PrePublishStep;
import org.apache.gobblin.data.management.copy.recovery.RecoveryHelper;
import org.apache.gobblin.data.management.copy.splitter.DistcpFileSplitter;
import org.apache.gobblin.data.management.copy.writer.FileAwareInputStreamDataWriter;
import org.apache.gobblin.data.management.copy.writer.FileAwareInputStreamDataWriterBuilder;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.GobblinMetrics;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.metrics.event.sla.SlaEventKeys;
import org.apache.gobblin.publisher.DataPublisher;
import org.apache.gobblin.publisher.UnpublishedHandling;
import org.apache.gobblin.util.HadoopUtils;
import org.apache.gobblin.util.WriterUtils;


/**
 * A {@link DataPublisher} to {@link org.apache.gobblin.data.management.copy.CopyEntity}s from task output to final destination.
 */
@Slf4j
public class CopyDataPublisher extends DataPublisher implements UnpublishedHandling {

  @Override
  public boolean isThreadSafe() {
    return this.getClass() == CopyDataPublisher.class;
  }
  private final FileSystem srcFs;
  private final FileSystem fs;
  protected final EventSubmitter eventSubmitter;
  protected final RecoveryHelper recoveryHelper;
  protected final Optional<LineageInfo> lineageInfo;
  protected final DataFileVersionStrategy srcDataFileVersionStrategy;
  protected final DataFileVersionStrategy dstDataFileVersionStrategy;

  /**
   * Build a new {@link CopyDataPublisher} from {@link State}. The constructor expects the following to be set in the
   * {@link State},
   * <ul>
   * <li>{@link ConfigurationKeys#WRITER_OUTPUT_DIR}
   * <li>{@link ConfigurationKeys#WRITER_FILE_SYSTEM_URI}
   * </ul>
   *
   */
  public CopyDataPublisher(State state) throws IOException {
    super(state);
    // Extract LineageInfo from state
    if (state instanceof SourceState) {
      lineageInfo = LineageInfo.getLineageInfo(((SourceState) state).getBroker());
    } else if (state instanceof WorkUnitState) {
      lineageInfo = LineageInfo.getLineageInfo(((WorkUnitState) state).getTaskBrokerNullable());
    } else {
      lineageInfo = Optional.absent();
    }

    String uri = this.state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    this.fs = FileSystem.get(URI.create(uri), WriterUtils.getFsConfiguration(state));

    MetricContext metricContext =
        Instrumented.getMetricContext(state, CopyDataPublisher.class, GobblinMetrics.getCustomTagsFromState(state));

    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "org.apache.gobblin.copy.CopyDataPublisher").build();

    this.recoveryHelper = new RecoveryHelper(this.fs, state);
    this.recoveryHelper.purgeOldPersistedFile();

    Config config = ConfigUtils.propertiesToConfig(state.getProperties());

    this.srcFs = HadoopUtils.getSourceFileSystem(state);
    this.srcDataFileVersionStrategy = DataFileVersionStrategy.instantiateDataFileVersionStrategy(this.srcFs, config);
    this.dstDataFileVersionStrategy = DataFileVersionStrategy.instantiateDataFileVersionStrategy(this.fs, config);
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    /*
     * This mapping is used to set WorkingState of all {@link WorkUnitState}s to {@link
     * WorkUnitState.WorkingState#COMMITTED} after a {@link CopyableDataset} is successfully published
     */
    Multimap<CopyEntity.DatasetAndPartition, WorkUnitState> datasets = groupByFileSet(states);

    boolean allDatasetsPublished = true;
    for (CopyEntity.DatasetAndPartition datasetAndPartition : datasets.keySet()) {
      try {
        this.publishFileSet(datasetAndPartition, datasets.get(datasetAndPartition));
      } catch (Throwable e) {
        CopyEventSubmitterHelper.submitFailedDatasetPublish(this.eventSubmitter, datasetAndPartition);
        log.error("Failed to publish " + datasetAndPartition.getDataset().getDatasetURN(), e);
        allDatasetsPublished = false;
      }
    }

    if (!allDatasetsPublished) {
      throw new IOException("Not all datasets published successfully");
    }
  }

  @Override
  public void handleUnpublishedWorkUnits(Collection<? extends WorkUnitState> states) throws IOException {
    int filesPersisted = persistFailedFileSet(states);
    log.info(String.format("Successfully persisted %d work units.", filesPersisted));
  }

  /**
   * Create a {@link Multimap} that maps a {@link CopyableDataset} to all {@link WorkUnitState}s that belong to this
   * {@link CopyableDataset}. This mapping is used to set WorkingState of all {@link WorkUnitState}s to
   * {@link WorkUnitState.WorkingState#COMMITTED} after a {@link CopyableDataset} is successfully published.
   */
  private static Multimap<CopyEntity.DatasetAndPartition, WorkUnitState> groupByFileSet(
      Collection<? extends WorkUnitState> states) {
    Multimap<CopyEntity.DatasetAndPartition, WorkUnitState> datasetRoots = ArrayListMultimap.create();

    for (WorkUnitState workUnitState : states) {
      CopyEntity file = CopySource.deserializeCopyEntity(workUnitState);
      CopyEntity.DatasetAndPartition datasetAndPartition = file.getDatasetAndPartition(
          CopyableDatasetMetadata.deserialize(workUnitState.getProp(CopySource.SERIALIZED_COPYABLE_DATASET)));

      datasetRoots.put(datasetAndPartition, workUnitState);
    }
    return datasetRoots;
  }

  /**
   * Unlike other preserving attributes of files (ownership, group, etc.), which is preserved in writer,
   * some of the attributes have to be set during publish phase like ModTime,
   * and versionStrategy (usually relevant to modTime as well), since they are subject to change with Publish(rename)
   */
  private void preserveFileAttrInPublisher(CopyableFile copyableFile) throws IOException {
    // Preserving File ModTime, and set the access time to an initializing value when ModTime is declared to be preserved.
    if (copyableFile.getPreserve().preserve(PreserveAttributes.Option.MOD_TIME)) {
      fs.setTimes(copyableFile.getDestination(), copyableFile.getOriginTimestamp(), -1);
    }

    // Preserving File Version.
    DataFileVersionStrategy srcVS = this.srcDataFileVersionStrategy;
    DataFileVersionStrategy dstVS = this.dstDataFileVersionStrategy;

    // Prefer to use copyableFile's specific version strategy
    if (copyableFile.getDataFileVersionStrategy() != null) {
      Config versionStrategyConfig = ConfigFactory.parseMap(ImmutableMap.of(
          DataFileVersionStrategy.DATA_FILE_VERSION_STRATEGY_KEY, copyableFile.getDataFileVersionStrategy()));
      srcVS = DataFileVersionStrategy.instantiateDataFileVersionStrategy(this.srcFs, versionStrategyConfig);
      dstVS = DataFileVersionStrategy.instantiateDataFileVersionStrategy(this.fs, versionStrategyConfig);
    }

    if (copyableFile.getPreserve().preserve(PreserveAttributes.Option.VERSION)
        && dstVS.hasCharacteristic(DataFileVersionStrategy.Characteristic.SETTABLE)) {
      dstVS.setVersion(copyableFile.getDestination(),
          srcVS.getVersion(copyableFile.getOrigin().getPath()));
    }
  }

  /**
   * Publish data for a {@link CopyableDataset}.
   */
  private void publishFileSet(CopyEntity.DatasetAndPartition datasetAndPartition,
      Collection<WorkUnitState> datasetWorkUnitStates) throws IOException {
    Map<String, String> additionalMetadata = Maps.newHashMap();

    Preconditions.checkArgument(!datasetWorkUnitStates.isEmpty(),
        "publishFileSet received an empty collection work units. This is an error in code.");

    WorkUnitState sampledWorkUnitState =  datasetWorkUnitStates.iterator().next();

    CopyableDatasetMetadata metadata = CopyableDatasetMetadata.deserialize(
        sampledWorkUnitState.getProp(CopySource.SERIALIZED_COPYABLE_DATASET));

    // If not already done, ensure that the writer outputs have the job ID appended to avoid corruption from previous runs
    FileAwareInputStreamDataWriterBuilder.setJobSpecificOutputPaths(sampledWorkUnitState);
    Path writerOutputDir = new Path(sampledWorkUnitState.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));

    Path datasetWriterOutputPath = new Path(writerOutputDir, datasetAndPartition.identifier());

    log.info("Merging all split work units.");
    DistcpFileSplitter.mergeAllSplitWorkUnits(this.fs, datasetWorkUnitStates);

    log.info(String.format("[%s] Publishing fileSet from %s for dataset %s", datasetAndPartition.identifier(),
        datasetWriterOutputPath, metadata.getDatasetURN()));

    List<CommitStep> prePublish = getCommitSequence(datasetWorkUnitStates, PrePublishStep.class);
    List<CommitStep> postPublish = getCommitSequence(datasetWorkUnitStates, PostPublishStep.class);
    log.info(String.format("[%s] Found %d prePublish steps and %d postPublish steps.", datasetAndPartition.identifier(),
        prePublish.size(), postPublish.size()));

    executeCommitSequence(prePublish);
    if (hasCopyableFiles(datasetWorkUnitStates)) {
      // Targets are always absolute, so we start moving from root (will skip any existing directories).
      HadoopUtils.renameRecursively(this.fs, datasetWriterOutputPath, new Path("/"));
    } else {
      log.info(String.format("[%s] No copyable files in dataset. Proceeding to postpublish steps.", datasetAndPartition.identifier()));
    }
    executeCommitSequence(postPublish);

    this.fs.delete(datasetWriterOutputPath, true);

    long datasetOriginTimestamp = Long.MAX_VALUE;
    long datasetUpstreamTimestamp = Long.MAX_VALUE;
    Optional<String> fileSetRoot = Optional.<String>absent();

    for (WorkUnitState wus : datasetWorkUnitStates) {
      if (wus.getWorkingState() == WorkingState.SUCCESSFUL) {
        wus.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      }
      CopyEntity copyEntity = CopySource.deserializeCopyEntity(wus);
      if (copyEntity instanceof CopyableFile) {
        CopyableFile copyableFile = (CopyableFile) copyEntity;
        preserveFileAttrInPublisher(copyableFile);
        if (wus.getWorkingState() == WorkingState.COMMITTED) {
          CopyEventSubmitterHelper.submitSuccessfulFilePublish(this.eventSubmitter, copyableFile, wus);
          // Dataset Output path is injected in each copyableFile.
          // This can be optimized by having a dataset level equivalent class for copyable entities
          // and storing dataset related information, e.g. dataset output path, there.

          // Currently datasetOutputPath is only present for hive datasets.
          if (!fileSetRoot.isPresent() && copyableFile.getDatasetOutputPath() != null) {
            fileSetRoot = Optional.of(copyableFile.getDatasetOutputPath());
          }
          if (lineageInfo.isPresent()) {
            lineageInfo.get().putDestination(copyableFile.getDestinationData(), 0, wus);
          }
        }
        if (datasetOriginTimestamp > copyableFile.getOriginTimestamp()) {
          datasetOriginTimestamp = copyableFile.getOriginTimestamp();
        }
        if (datasetUpstreamTimestamp > copyableFile.getUpstreamTimestamp()) {
          datasetUpstreamTimestamp = copyableFile.getUpstreamTimestamp();
        }
      }
    }

    // if there are no valid values for datasetOriginTimestamp and datasetUpstreamTimestamp, use
    // something more readable
    if (Long.MAX_VALUE == datasetOriginTimestamp) {
      datasetOriginTimestamp = 0;
    }
    if (Long.MAX_VALUE == datasetUpstreamTimestamp) {
      datasetUpstreamTimestamp = 0;
    }

    additionalMetadata.put(SlaEventKeys.SOURCE_URI, this.state.getProp(SlaEventKeys.SOURCE_URI));
    additionalMetadata.put(SlaEventKeys.DESTINATION_URI, this.state.getProp(SlaEventKeys.DESTINATION_URI));
    additionalMetadata.put(SlaEventKeys.DATASET_OUTPUT_PATH, fileSetRoot.or("Unknown"));
    CopyEventSubmitterHelper.submitSuccessfulDatasetPublish(this.eventSubmitter, datasetAndPartition,
        Long.toString(datasetOriginTimestamp), Long.toString(datasetUpstreamTimestamp), additionalMetadata);
  }


  private static boolean hasCopyableFiles(Collection<WorkUnitState> workUnits) throws IOException {
    for (WorkUnitState wus : workUnits) {
      if (CopyableFile.class.isAssignableFrom(CopySource.getCopyEntityClass(wus))) {
        return true;
      }
    }
    return false;
  }

  private static List<CommitStep> getCommitSequence(Collection<WorkUnitState> workUnits, Class<?> baseClass)
      throws IOException {
    List<CommitStepCopyEntity> steps = Lists.newArrayList();
    for (WorkUnitState wus : workUnits) {
      if (baseClass.isAssignableFrom(CopySource.getCopyEntityClass(wus))) {
        CommitStepCopyEntity step = (CommitStepCopyEntity) CopySource.deserializeCopyEntity(wus);
        steps.add(step);
      }
    }

    Comparator<CommitStepCopyEntity> commitStepSorter = new Comparator<CommitStepCopyEntity>() {
      @Override
      public int compare(CommitStepCopyEntity o1, CommitStepCopyEntity o2) {
        return Integer.compare(o1.getPriority(), o2.getPriority());
      }
    };

    Collections.sort(steps, commitStepSorter);
    List<CommitStep> sequence = Lists.newArrayList();
    for (CommitStepCopyEntity entity : steps) {
      sequence.add(entity.getStep());
    }

    return sequence;
  }

  private static void executeCommitSequence(List<CommitStep> steps) throws IOException {
    for (CommitStep step : steps) {
      step.execute();
    }
  }

  private int persistFailedFileSet(Collection<? extends WorkUnitState> workUnitStates) throws IOException {
    int filesPersisted = 0;
    for (WorkUnitState wu : workUnitStates) {
      if (wu.getWorkingState() == WorkingState.SUCCESSFUL) {
        CopyEntity entity = CopySource.deserializeCopyEntity(wu);
        if (entity instanceof CopyableFile) {
          CopyableFile file = (CopyableFile) entity;
          Path outputDir = FileAwareInputStreamDataWriter.getOutputDir(wu);
          CopyableDatasetMetadata metadata = CopySource.deserializeCopyableDataset(wu);
          Path outputPath =
              FileAwareInputStreamDataWriter.getOutputFilePath(file, outputDir, file.getDatasetAndPartition(metadata));
          if (this.recoveryHelper.persistFile(wu, file, outputPath)) {
            filesPersisted++;
          }
        }
      }
    }
    return filesPersisted;
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {}

  @Override
  public void close() throws IOException {}

  @Override
  public void initialize() throws IOException {}
}
