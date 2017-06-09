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

package gobblin.data.management.copy.publisher;


import gobblin.metrics.event.sla.SlaEventKeys;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import gobblin.commit.CommitStep;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.data.management.copy.CopyEntity;
import gobblin.data.management.copy.CopySource;
import gobblin.data.management.copy.CopyableDataset;
import gobblin.data.management.copy.CopyableDatasetMetadata;
import gobblin.data.management.copy.CopyableFile;
import gobblin.data.management.copy.entities.CommitStepCopyEntity;
import gobblin.data.management.copy.entities.PostPublishStep;
import gobblin.data.management.copy.entities.PrePublishStep;
import gobblin.data.management.copy.recovery.RecoveryHelper;
import gobblin.data.management.copy.writer.FileAwareInputStreamDataWriter;
import gobblin.data.management.copy.writer.FileAwareInputStreamDataWriterBuilder;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.GobblinMetrics;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.publisher.DataPublisher;
import gobblin.publisher.UnpublishedHandling;
import gobblin.util.HadoopUtils;
import gobblin.util.WriterUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * A {@link DataPublisher} to {@link gobblin.data.management.copy.CopyEntity}s from task output to final destination.
 */
@Slf4j
public class CopyDataPublisher extends DataPublisher implements UnpublishedHandling {

  private final Path writerOutputDir;

  @Override
  public boolean isThreadSafe() {
    return this.getClass() == CopyDataPublisher.class;
  }

  private final FileSystem fs;
  protected final EventSubmitter eventSubmitter;
  protected final RecoveryHelper recoveryHelper;

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
    String uri = this.state.getProp(ConfigurationKeys.WRITER_FILE_SYSTEM_URI, ConfigurationKeys.LOCAL_FS_URI);
    this.fs = FileSystem.get(URI.create(uri), WriterUtils.getFsConfiguration(state));

    FileAwareInputStreamDataWriterBuilder.setJobSpecificOutputPaths(state);

    this.writerOutputDir = new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR));

    MetricContext metricContext =
        Instrumented.getMetricContext(state, CopyDataPublisher.class, GobblinMetrics.getCustomTagsFromState(state));

    this.eventSubmitter = new EventSubmitter.Builder(metricContext, "gobblin.copy.CopyDataPublisher").build();

    this.recoveryHelper = new RecoveryHelper(this.fs, state);
    this.recoveryHelper.purgeOldPersistedFile();
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
   * Publish data for a {@link CopyableDataset}.
   *
   */
  private void publishFileSet(CopyEntity.DatasetAndPartition datasetAndPartition,
      Collection<WorkUnitState> datasetWorkUnitStates) throws IOException {
    Map<String, String> additionalMetadata = Maps.newHashMap();

    Preconditions.checkArgument(!datasetWorkUnitStates.isEmpty(),
        "publishFileSet received an empty collection work units. This is an error in code.");

    CopyableDatasetMetadata metadata = CopyableDatasetMetadata
        .deserialize(datasetWorkUnitStates.iterator().next().getProp(CopySource.SERIALIZED_COPYABLE_DATASET));
    Path datasetWriterOutputPath = new Path(this.writerOutputDir, datasetAndPartition.identifier());

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

    for (WorkUnitState wus : datasetWorkUnitStates) {
      if (wus.getWorkingState() == WorkingState.SUCCESSFUL) {
        wus.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
      }
      CopyEntity copyEntity = CopySource.deserializeCopyEntity(wus);
      if (copyEntity instanceof CopyableFile) {
        CopyableFile copyableFile = (CopyableFile) copyEntity;
        if (wus.getWorkingState() == WorkingState.COMMITTED) {
          CopyEventSubmitterHelper.submitSuccessfulFilePublish(this.eventSubmitter, copyableFile, wus);
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
