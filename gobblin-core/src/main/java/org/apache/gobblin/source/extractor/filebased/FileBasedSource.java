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

package org.apache.gobblin.source.extractor.filebased;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.Extract.TableType;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


/**
 * This class is a base class for file based sources, it provides default
 * functionality for keeping track of which files have already been pulled
 * by the framework and for determining which files need to be pulled in this run
 * @author stakiar
 */
public abstract class FileBasedSource<S, D> extends AbstractSource<S, D> {
  private static final Logger log = LoggerFactory.getLogger(FileBasedSource.class);
  protected TimestampAwareFileBasedHelper fsHelper;
  protected String splitPattern = ":::";

  protected Optional<LineageInfo> lineageInfo;

  /**
   * Initialize the logger.
   *
   * @param state Source state
   */
  protected void initLogger(SourceState state) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(Strings.nullToEmpty(state.getProp(ConfigurationKeys.SOURCE_ENTITY)));
    sb.append("]");
    MDC.put("sourceInfo", sb.toString());
  }

  /**
   * This method takes the snapshot seen in the previous run, and compares it to the list
   * of files currently in the source - it then decided which files it needs to pull
   * and distributes those files across the workunits; it does this comparison by comparing
   * the names of the files currently in the source vs. the names retrieved from the
   * previous state
   * @param state is the source state
   * @return a list of workunits for the framework to run
   */
  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    initLogger(state);
    lineageInfo = LineageInfo.getLineageInfo(state.getBroker());

    try {
      initFileSystemHelper(state);
    } catch (FileBasedHelperException e) {
      Throwables.propagate(e);
    }

    log.info("Getting work units");
    String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    String entityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);

    // Override extract table name
    String extractTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);

    // If extract table name is not found then consider entity name as extract table name
    if (Strings.isNullOrEmpty(extractTableName)) {
      extractTableName = entityName;
    }

    TableType tableType = TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());
    List<WorkUnitState> previousWorkunits = Lists.newArrayList(state.getPreviousWorkUnitStates());
    Set<String> prevFsSnapshot = Sets.newHashSet();

    // Get list of files seen in the previous run
    if (!previousWorkunits.isEmpty()) {
      if (previousWorkunits.get(0).getWorkunit().contains(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT)) {
        prevFsSnapshot.addAll(previousWorkunits.get(0).getWorkunit().getPropAsSet(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT));
      } else if (state.getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED,
            ConfigurationKeys.DEFAULT_SOURCE_FILEBASED_FS_PRIOR_SNAPSHOT_REQUIRED)) {
        // If a previous job exists, there should be a snapshot property.  If not, we need to fail so that we
        // don't accidentally read files that have already been processed.
        throw new RuntimeException(String.format("No '%s' found on state of prior job",
            ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT));
      }
    }

    List<WorkUnit> workUnits = Lists.newArrayList();
    List<WorkUnit> previousWorkUnitsForRetry = this.getPreviousWorkUnitsForRetry(state);
    log.info("Total number of work units from the previous failed runs: " + previousWorkUnitsForRetry.size());
    for (WorkUnit previousWorkUnitForRetry : previousWorkUnitsForRetry) {
      prevFsSnapshot.addAll(previousWorkUnitForRetry.getPropAsSet(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
      workUnits.add(previousWorkUnitForRetry);
    }

    // Get list of files that need to be pulled
    List<String> currentFsSnapshot = this.getcurrentFsSnapshot(state);
    // The snapshot we want to save. This might not be the full snapshot if we don't pull all files.
    List<String> effectiveSnapshot = Lists.newArrayList();
    List<String> filesToPull = Lists.newArrayList();

    int maxFilesToPull = state.getPropAsInt(ConfigurationKeys.SOURCE_FILEBASED_MAX_FILES_PER_RUN, Integer.MAX_VALUE);
    int filesSelectedForPull = 0;
    if (currentFsSnapshot.size() > maxFilesToPull) {
      // if we're going to not pull all files, sort them lexicographically so there is some order in which they are ingested
      // note currentFsSnapshot.size > maxFilesToPull does not imply we will ignore some of them, as we still have to diff
      // against the previous snapshot. Just a quick check if it even makes sense to sort the files.
      Collections.sort(currentFsSnapshot);
    }
    for (String file: currentFsSnapshot) {
      if (prevFsSnapshot.contains(file)) {
        effectiveSnapshot.add(file);
      } else if ((filesSelectedForPull++) < maxFilesToPull) {
        filesToPull.add(file.split(this.splitPattern)[0]);
        effectiveSnapshot.add(file);
      } else {
        // file is not pulled this run
      }
    }
    // Update the snapshot from the previous run with the new files processed in this run
    // Otherwise a corrupt file could cause re-processing of already processed files
    for (WorkUnit workUnit : previousWorkUnitsForRetry) {
      workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT, StringUtils.join(effectiveSnapshot, ","));
    }

    if (!filesToPull.isEmpty()) {
      logFilesToPull(filesToPull);

      int numPartitions = state.contains(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS)
          && state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) <= filesToPull.size()
              ? state.getPropAsInt(ConfigurationKeys.SOURCE_MAX_NUMBER_OF_PARTITIONS) : filesToPull.size();
      if (numPartitions <= 0) {
        throw new IllegalArgumentException("The number of partitions should be positive");
      }

      int filesPerPartition = filesToPull.size() % numPartitions == 0 ? filesToPull.size() / numPartitions
          : filesToPull.size() / numPartitions + 1;

      // Distribute the files across the workunits
      for (int fileOffset = 0; fileOffset < filesToPull.size(); fileOffset += filesPerPartition) {
        /* Use extract table name to create extract
         *
         * We don't want to pass in the whole SourceState object just to avoid any side effect, because
         * the constructor with state argument has been deprecated for a long time. Here we selectively
         * chose the configuration needed for Extract constructor, to manually form a source state.
         */
        SourceState extractState = new SourceState();
        extractState.setProp(ConfigurationKeys.EXTRACT_ID_TIME_ZONE,
                state.getProp(ConfigurationKeys.EXTRACT_ID_TIME_ZONE, ConfigurationKeys.DEFAULT_EXTRACT_ID_TIME_ZONE));
        extractState.setProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY,
                state.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY, ConfigurationKeys.DEFAULT_EXTRACT_IS_FULL));
        Extract extract = new Extract(extractState, tableType, nameSpaceName, extractTableName);

        WorkUnit workUnit = WorkUnit.create(extract);

        // Eventually these setters should be integrated with framework support for generalized watermark handling
        workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FS_SNAPSHOT,
            StringUtils.join(effectiveSnapshot, ","));

        List<String> partitionFilesToPull = filesToPull.subList(fileOffset,
            fileOffset + filesPerPartition > filesToPull.size() ? filesToPull.size() : fileOffset + filesPerPartition);
        workUnit.setProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL,
            StringUtils.join(partitionFilesToPull, ","));
        if (state.getPropAsBoolean(ConfigurationKeys.SOURCE_FILEBASED_PRESERVE_FILE_NAME, false)) {
          if (partitionFilesToPull.size() != 1) {
            throw new RuntimeException("Cannot preserve the file name if a workunit is given multiple files");
          }
          workUnit.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR,
              workUnit.getProp(ConfigurationKeys.SOURCE_FILEBASED_FILES_TO_PULL));
        }

        workUnits.add(workUnit);
      }

      log.info("Total number of work units for the current run: " + (workUnits.size() - previousWorkUnitsForRetry.size()));
    }

    addLineageSourceInfo(workUnits, state);
    return workUnits;
  }

  /**
   * Add lineage source info to a list of work units, it can have instances of
   * {@link org.apache.gobblin.source.workunit.MultiWorkUnit}
   */
  protected void addLineageSourceInfo(List<WorkUnit> workUnits, State state) {
    workUnits.forEach(workUnit -> {
      if (workUnit instanceof MultiWorkUnit) {
        ((MultiWorkUnit) workUnit).getWorkUnits().forEach((wu -> addLineageSourceInfo(wu, state)));
      } else {
        addLineageSourceInfo(workUnit, state);
      }
    });
  }

  /**
   * Add lineage source info to a single work unit
   *
   * @param workUnit a single work unit, not an instance of {@link org.apache.gobblin.source.workunit.MultiWorkUnit}
   * @param state configurations
   */
  protected void addLineageSourceInfo(WorkUnit workUnit, State state) {
    if (!lineageInfo.isPresent()) {
      log.info("Lineage is not enabled");
      return;
    }

    String platform = state.getProp(ConfigurationKeys.SOURCE_FILEBASED_PLATFORM, DatasetConstants.PLATFORM_HDFS);
    Path dataDir = new Path(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY));
    String dataset = Path.getPathWithoutSchemeAndAuthority(dataDir).toString();
    URI fileSystemUrl =
        URI.create(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_FS_URI, ConfigurationKeys.LOCAL_FS_URI));
    DatasetDescriptor source = new DatasetDescriptor(platform, fileSystemUrl, dataset);
    lineageInfo.get().setSource(source, workUnit);
  }

  /**
   * This method is responsible for connecting to the source and taking
   * a snapshot of the folder where the data is present, it then returns
   * a list of the files in String format
   * @param state is used to connect to the source
   * @return a list of file name or paths present on the external data
   * directory
   */
  public List<String> getcurrentFsSnapshot(State state) {
    List<String> results;
    String path = getLsPattern(state);

    try {
      log.info("Running ls command with input " + path);
      results = this.fsHelper.ls(path);
      for (int i = 0; i < results.size(); i++) {
        URI uri = new URI(results.get(i));
        String filePath = uri.toString();
        if (!uri.isAbsolute()) {
          File file = new File(state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY), uri.toString());
          filePath = file.getAbsolutePath();
        }
        results.set(i, filePath + this.splitPattern + this.fsHelper.getFileMTime(filePath));
      }
    } catch (FileBasedHelperException | URISyntaxException e) {
      String errMsg = String.format(
          "Not able to fetch the filename/file modified time to %s. Will not pull any files", e.getMessage());
      log.error(errMsg, e);
      throw new RuntimeException(errMsg, e);
    }
    return results;
  }

  protected String getLsPattern(State state) {
    return state.getProp(ConfigurationKeys.SOURCE_FILEBASED_DATA_DIRECTORY) + "/*"
        + state.getProp(ConfigurationKeys.SOURCE_ENTITY) + "*";
  }

  @Override
  public void shutdown(SourceState state) {
    if (this.fsHelper != null) {
      log.info("Shutting down the FileSystemHelper connection");
      try {
        this.fsHelper.close();
      } catch (IOException e) {
        log.error("Unable to shutdown FileSystemHelper", e);
      }
    }
  }

  public abstract void initFileSystemHelper(State state) throws FileBasedHelperException;

  private void logFilesToPull(List<String> filesToPull) {
    int filesToLog = Math.min(2000, filesToPull.size());
    String remainingString = "";
    if (filesToLog < filesToPull.size()) {
      remainingString = "and " + (filesToPull.size() - filesToLog) + " more ";
    }
    log.info(String.format("Will pull the following files %s in this run: %s", remainingString,
            Arrays.toString(filesToPull.subList(0, filesToLog).toArray())));
  }
}
