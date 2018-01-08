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

package org.apache.gobblin.source.extractor.extract;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.gobblin.config.client.ConfigClient;
import org.apache.gobblin.config.client.ConfigClientCache;
import org.apache.gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import org.apache.gobblin.config.client.api.VersionStabilityPolicy;
import org.apache.gobblin.config.store.api.ConfigStoreCreationException;
import org.apache.gobblin.config.store.api.VersionDoesNotExistException;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.configuration.WorkUnitState.WorkingState;
import org.apache.gobblin.metrics.event.lineage.LineageInfo;
import org.apache.gobblin.source.extractor.JobCommitPolicy;
import org.apache.gobblin.source.extractor.partition.Partition;
import org.apache.gobblin.source.extractor.partition.Partitioner;
import org.apache.gobblin.source.extractor.utils.Utils;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.Extract.TableType;
import org.apache.gobblin.source.workunit.MultiWorkUnit;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.DatasetFilterUtils;
import org.apache.gobblin.util.PathUtils;
import org.apache.gobblin.util.dataset.DatasetUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;


/**
 * A base implementation of {@link org.apache.gobblin.source.Source} for
 * query-based sources.
 */
@Slf4j
public abstract class QueryBasedSource<S, D> extends AbstractSource<S, D> {

  public static final String ENTITY_BLACKLIST = "entity.blacklist";
  public static final String ENTITY_WHITELIST = "entity.whitelist";
  public static final String SOURCE_OBTAIN_TABLE_PROPS_FROM_CONFIG_STORE =
      "source.obtain_table_props_from_config_store";
  public static final boolean DEFAULT_SOURCE_OBTAIN_TABLE_PROPS_FROM_CONFIG_STORE = false;
  private static final String QUERY_BASED_SOURCE = "query_based_source";
  public static final String WORK_UNIT_STATE_VERSION_KEY = "source.querybased.workUnitState.version";
  /**
   * WorkUnit Version 3:
   *    SOURCE_ENTITY = as specified in job config
   *    EXTRACT_TABLE_NAME_KEY = as specified in job config or sanitized version of SOURCE_ENTITY
   * WorkUnit Version 2 (implicit):
   *    SOURCE_ENTITY = sanitized version of SOURCE_ENTITY in job config
   *    EXTRACT_TABLE_NAME_KEY = as specified in job config
   * WorkUnit Version 1 (implicit):
   *    SOURCE_ENTITY = as specified in job config
   *    EXTRACT_TABLE_NAME_KEY = as specified in job config
   */
  public static final Integer CURRENT_WORK_UNIT_STATE_VERSION = 3;

  protected Optional<LineageInfo> lineageInfo;

  /** A class that encapsulates a source entity (aka dataset) to be processed */
  @Data
  public static final class SourceEntity {
    /**
     * The name of the source entity (as specified in the source) to be processed. For example,
     * this can be a table name.
     */
    private final String sourceEntityName;
    /**
     * The destination table name. This is explicitly specified in the config or is derived from
     *  the sourceEntityName.
     */
    private final String destTableName;

    /** A string that identifies the source entity */
    public String getDatasetName() {
      return sourceEntityName;
    }

    static String sanitizeEntityName(String entity) {
      return Utils.escapeSpecialCharacters(entity, ConfigurationKeys.ESCAPE_CHARS_IN_TABLE_NAME, "_");
    }

    public static SourceEntity fromSourceEntityName(String sourceEntityName) {
      return new SourceEntity(sourceEntityName, sanitizeEntityName(sourceEntityName));
    }

    public static Optional<SourceEntity> fromState(State state) {
      String sourceEntityName;
      String destTableName;
      if (state.contains(ConfigurationKeys.SOURCE_ENTITY)) {
        sourceEntityName = state.getProp(ConfigurationKeys.SOURCE_ENTITY);
        destTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY,
            sanitizeEntityName(sourceEntityName));
      }
      else if (state.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY)) {
        destTableName = state.getProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY);
        sourceEntityName = destTableName;
      }
      else {
        return Optional.absent();
      }

      return Optional.of(new SourceEntity(sourceEntityName, destTableName));
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      SourceEntity other = (SourceEntity) obj;
      if (getDatasetName() == null) {
        if (other.getDatasetName() != null)
          return false;
      } else if (!getDatasetName().equals(other.getDatasetName()))
        return false;
      return true;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((getDatasetName() == null) ? 0 : getDatasetName().hashCode());
      return result;
    }
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    initLogger(state);
    lineageInfo = LineageInfo.getLineageInfo(state.getBroker());

    List<WorkUnit> workUnits = Lists.newArrayList();

    // Map<String, String> tableNameToEntityMap = Maps.newHashMap();
    Set<SourceEntity> entities = getFilteredSourceEntities(state);

    Map<SourceEntity, State> tableSpecificPropsMap = shouldObtainTablePropsFromConfigStore(state)
        ? getTableSpecificPropsFromConfigStore(entities, state)
        : getTableSpecificPropsFromState(entities, state);
    Map<SourceEntity, Long> prevWatermarksByTable = getPreviousWatermarksForAllTables(state);

    for (SourceEntity sourceEntity : Sets.union(entities, prevWatermarksByTable.keySet())) {

      log.info("Source entity to be processed: {}, carry-over from previous state: {} ",
               sourceEntity, !entities.contains(sourceEntity));

      SourceState combinedState = getCombinedState(state, tableSpecificPropsMap.get(sourceEntity));
      long previousWatermark = prevWatermarksByTable.containsKey(sourceEntity) ?
          prevWatermarksByTable.get(sourceEntity)
          : ConfigurationKeys.DEFAULT_WATERMARK_VALUE;

      // If a table name exists in prevWatermarksByTable (i.e., it has a previous watermark) but does not exist
      // in talbeNameToEntityMap, create an empty workunit for it, so that its previous watermark is preserved.
      // This is done by overriding the high watermark to be the same as the previous watermark.
      if (!entities.contains(sourceEntity)) {
        combinedState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, previousWatermark);
      }

      workUnits.addAll(generateWorkUnits(sourceEntity, state, previousWatermark));
    }

    log.info("Total number of workunits for the current run: " + workUnits.size());
    List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
    log.info("Total number of incomplete tasks from the previous run: " + previousWorkUnits.size());
    workUnits.addAll(previousWorkUnits);

    int numOfMultiWorkunits =
        state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);

    return pack(workUnits, numOfMultiWorkunits);
  }

  protected List<WorkUnit> generateWorkUnits(SourceEntity sourceEntity, SourceState state, long previousWatermark) {
    List<WorkUnit> workUnits = Lists.newArrayList();

    String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);
    TableType tableType =
        TableType.valueOf(state.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());

    List<Partition> partitions = new Partitioner(state).getPartitionList(previousWatermark);
    Collections.sort(partitions, Partitioner.ascendingComparator);

    // {@link ConfigurationKeys.EXTRACT_TABLE_NAME_KEY} specify the output path for Extract
    String outputTableName = sourceEntity.getDestTableName();

    log.info("Create extract output with table name is " + outputTableName);
    Extract extract = createExtract(tableType, nameSpaceName, outputTableName);

    // Setting current time for the full extract
    if (Boolean.valueOf(state.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY))) {
      extract.setFullTrue(System.currentTimeMillis());
    }

    for (Partition partition : partitions) {
      WorkUnit workunit = WorkUnit.create(extract);
      workunit.setProp(ConfigurationKeys.SOURCE_ENTITY, sourceEntity.getSourceEntityName());
      workunit.setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, sourceEntity.getDestTableName());
      workunit.setProp(WORK_UNIT_STATE_VERSION_KEY, CURRENT_WORK_UNIT_STATE_VERSION);
      addLineageSourceInfo(state, sourceEntity, workunit);
      partition.serialize(workunit);
      workUnits.add(workunit);
    }

    return workUnits;
  }

  protected void addLineageSourceInfo(SourceState sourceState, SourceEntity entity, WorkUnit workUnit) {
    // Does nothing by default
  }

  protected Set<SourceEntity> getFilteredSourceEntities(SourceState state) {
    Set<SourceEntity> unfilteredEntities = getSourceEntities(state);
    return getFilteredSourceEntitiesHelper(state, unfilteredEntities);
  }

  static Set<SourceEntity> getFilteredSourceEntitiesHelper(SourceState state, Iterable<SourceEntity> unfilteredEntities) {
    Set<SourceEntity> entities = new HashSet<>();
    List<Pattern> blacklist = DatasetFilterUtils.getPatternList(state, ENTITY_BLACKLIST);
    List<Pattern> whitelist = DatasetFilterUtils.getPatternList(state, ENTITY_WHITELIST);
    for (SourceEntity entity : unfilteredEntities) {
      if (DatasetFilterUtils.survived(entity.getSourceEntityName(), blacklist, whitelist)) {
        entities.add(entity);
      }
    }
    return entities;
  }

  public static Map<SourceEntity, State> getTableSpecificPropsFromState(
          Iterable<SourceEntity> entities,
          SourceState state) {
    Map<String, SourceEntity> sourceEntityByName = new HashMap<>();
    for (SourceEntity entity: entities) {
      sourceEntityByName.put(entity.getDatasetName(), entity);
    }
    Map<String, State> datasetProps =
        DatasetUtils.getDatasetSpecificProps(sourceEntityByName.keySet(), state);
    Map<SourceEntity, State> res = new HashMap<>();
    for (Map.Entry<String, State> entry: datasetProps.entrySet()) {
      res.put(sourceEntityByName.get(entry.getKey()), entry.getValue());
    }
    return res;
  }

  protected Set<SourceEntity> getSourceEntities(State state) {
    return getSourceEntitiesHelper(state);
  }

  static Set<SourceEntity> getSourceEntitiesHelper(State state) {
    if (state.contains(ConfigurationKeys.SOURCE_ENTITIES)) {
      log.info("Using entity names in " + ConfigurationKeys.SOURCE_ENTITIES);
      HashSet<SourceEntity> res = new HashSet<>();
      for (String sourceEntityName: state.getPropAsList(ConfigurationKeys.SOURCE_ENTITIES)) {
        res.add(SourceEntity.fromSourceEntityName(sourceEntityName));
      }
      return res;
    } else if (state.contains(ConfigurationKeys.SOURCE_ENTITY) ||
               state.contains(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY)) {
      Optional<SourceEntity> sourceEntity = SourceEntity.fromState(state);
      // Guaranteed to be present
      log.info("Using entity name in " + sourceEntity.get());
      return ImmutableSet.of(sourceEntity.get());
    }

    throw new IllegalStateException(String.format("One of the following properties must be specified: %s, %s.",
        ConfigurationKeys.SOURCE_ENTITIES, ConfigurationKeys.SOURCE_ENTITY));
  }

  private static boolean shouldObtainTablePropsFromConfigStore(SourceState state) {
    return state.getPropAsBoolean(SOURCE_OBTAIN_TABLE_PROPS_FROM_CONFIG_STORE,
        DEFAULT_SOURCE_OBTAIN_TABLE_PROPS_FROM_CONFIG_STORE);
  }

  private static Map<SourceEntity, State> getTableSpecificPropsFromConfigStore(
          Collection<SourceEntity> tables, State state) {
    ConfigClient client = ConfigClientCache.getClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY);
    String configStoreUri = state.getProp(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI);
    Preconditions.checkNotNull(configStoreUri);

    Map<SourceEntity, State> result = Maps.newHashMap();

    for (SourceEntity table : tables) {
      try {
        result.put(table, ConfigUtils.configToState(
            client.getConfig(PathUtils.combinePaths(configStoreUri, QUERY_BASED_SOURCE, table.getDatasetName()).toUri())));
      } catch (VersionDoesNotExistException | ConfigStoreFactoryDoesNotExistsException
          | ConfigStoreCreationException e) {
        throw new RuntimeException("Unable to get table config for " + table, e);
      }
    }

    return result;
  }

  private static SourceState getCombinedState(SourceState state, State tableSpecificState) {
    if (tableSpecificState == null) {
      return state;
    }
    SourceState combinedState =
        new SourceState(state, state.getPreviousDatasetStatesByUrns(), state.getPreviousWorkUnitStates());
    combinedState.addAll(tableSpecificState);
    return combinedState;
  }

  /**
   * Pack the list of {@code WorkUnit}s into {@code MultiWorkUnit}s.
   *
   * TODO: this is currently a simple round-robin packing. More sophisticated bin packing may be necessary
   * if the round-robin approach leads to mapper skew.
   */
  private static List<WorkUnit> pack(List<WorkUnit> workUnits, int numOfMultiWorkunits) {
    Preconditions.checkArgument(numOfMultiWorkunits > 0);

    if (workUnits.size() <= numOfMultiWorkunits) {
      return workUnits;
    }
    List<WorkUnit> result = Lists.newArrayListWithCapacity(numOfMultiWorkunits);
    for (int i = 0; i < numOfMultiWorkunits; i++) {
      result.add(MultiWorkUnit.createEmpty());
    }
    for (int i = 0; i < workUnits.size(); i++) {
      ((MultiWorkUnit) result.get(i % numOfMultiWorkunits)).addWorkUnit(workUnits.get(i));
    }
    return result;
  }

  @Override
  public void shutdown(SourceState state) {}


  /**
   * For each table, if job commit policy is to commit on full success, and the table has failed tasks in the
   * previous run, return the lowest low watermark among all previous {@code WorkUnitState}s of the table.
   * Otherwise, return the highest high watermark among all previous {@code WorkUnitState}s of the table.
   */
  static Map<SourceEntity, Long> getPreviousWatermarksForAllTables(SourceState state) {
    Map<SourceEntity, Long> result = Maps.newHashMap();
    Map<SourceEntity, Long> prevLowWatermarksByTable = Maps.newHashMap();
    Map<SourceEntity, Long> prevActualHighWatermarksByTable = Maps.newHashMap();
    Set<SourceEntity> tablesWithFailedTasks = Sets.newHashSet();
    Set<SourceEntity> tablesWithNoUpdatesOnPreviousRun = Sets.newHashSet();
    boolean commitOnFullSuccess = JobCommitPolicy.getCommitPolicy(state) == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS;

    for (WorkUnitState previousWus : state.getPreviousWorkUnitStates()) {
      Optional<SourceEntity> sourceEntity = SourceEntity.fromState(previousWus);
      if (!sourceEntity.isPresent()) {
        log.warn("Missing source entity for WorkUnit state: " + previousWus);
        continue;
      }
      SourceEntity table = sourceEntity.get();

      long lowWm = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
      LongWatermark waterMarkObj = previousWus.getWorkunit().getLowWatermark(LongWatermark.class);
      // new job state file(version 0.2.1270) , water mark format:
      // "watermark.interval.value": "{\"low.watermark.to.json\":{\"value\":20160101000000},\"expected.watermark.to.json\":{\"value\":20160715230234}}",
      if(waterMarkObj != null){
        lowWm = waterMarkObj.getValue();
      }
      // job state file(version 0.2.805)
      // "workunit.low.water.mark": "20160711000000",
      // "workunit.state.runtime.high.water.mark": "20160716140338",
      else if(previousWus.getProperties().containsKey(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY)){
        lowWm = Long.parseLong(previousWus.getProperties().getProperty(ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY));
        log.warn("can not find low water mark in json format, getting value from " + ConfigurationKeys.WORK_UNIT_LOW_WATER_MARK_KEY + " low water mark " + lowWm);
      }

      if (!prevLowWatermarksByTable.containsKey(table)) {
        prevLowWatermarksByTable.put(table, lowWm);
      } else {
        prevLowWatermarksByTable.put(table, Math.min(prevLowWatermarksByTable.get(table), lowWm));
      }

      long highWm = ConfigurationKeys.DEFAULT_WATERMARK_VALUE;
      waterMarkObj = previousWus.getActualHighWatermark(LongWatermark.class);
      if(waterMarkObj != null){
        highWm = waterMarkObj.getValue();
      }
      else if(previousWus.getProperties().containsKey(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK)){
        highWm = Long.parseLong(previousWus.getProperties().getProperty(ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK));
        log.warn("can not find high water mark in json format, getting value from " + ConfigurationKeys.WORK_UNIT_STATE_RUNTIME_HIGH_WATER_MARK + " high water mark " + highWm);
      }

      if (!prevActualHighWatermarksByTable.containsKey(table)) {
        prevActualHighWatermarksByTable.put(table, highWm);
      } else {
        prevActualHighWatermarksByTable.put(table, Math.max(prevActualHighWatermarksByTable.get(table), highWm));
      }

      if (commitOnFullSuccess && !isSuccessfulOrCommited(previousWus)) {
        tablesWithFailedTasks.add(table);
      }

      if (!isAnyDataProcessed(previousWus)) {
        tablesWithNoUpdatesOnPreviousRun.add(table);
      }
    }

    for (Map.Entry<SourceEntity, Long> entry : prevLowWatermarksByTable.entrySet()) {
      if (tablesWithFailedTasks.contains(entry.getKey())) {
        log.info("Resetting low watermark to {} because previous run failed.", entry.getValue());
        result.put(entry.getKey(), entry.getValue());
      } else if (tablesWithNoUpdatesOnPreviousRun.contains(entry.getKey())
          && state.getPropAsBoolean(ConfigurationKeys.SOURCE_QUERYBASED_RESET_EMPTY_PARTITION_WATERMARK,
          ConfigurationKeys.DEFAULT_SOURCE_QUERYBASED_RESET_EMPTY_PARTITION_WATERMARK)) {
        log.info("Resetting low watermakr to {} because previous run processed no data.", entry.getValue());
        result.put(entry.getKey(), entry.getValue());
      } else {
        result.put(entry.getKey(), prevActualHighWatermarksByTable.get(entry.getKey()));
      }
    }

    return result;
  }

  private static boolean isSuccessfulOrCommited(WorkUnitState wus) {
    return wus.getWorkingState() == WorkingState.SUCCESSFUL || wus.getWorkingState() == WorkingState.COMMITTED;
  }

  private static boolean isAnyDataProcessed(WorkUnitState wus) {
    return wus.getPropAsLong(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, 0) > 0;
  }

  /**
   * Initialize the logger.
   *
   * @param state
   *            Source state
   */
  private static void initLogger(SourceState state) {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append(StringUtils.stripToEmpty(state.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA)));
    sb.append("_");
    sb.append(StringUtils.stripToEmpty(state.getProp(ConfigurationKeys.SOURCE_ENTITY)));
    sb.append("]");
    MDC.put("sourceInfo", sb.toString());
  }
}
