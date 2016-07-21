/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.ConfigClientCache;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.config.store.api.VersionDoesNotExistException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.SourceState;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.source.extractor.JobCommitPolicy;
import gobblin.source.extractor.WatermarkInterval;
import gobblin.source.extractor.partition.Partitioner;
import gobblin.source.extractor.utils.Utils;
import gobblin.source.workunit.Extract;
import gobblin.source.workunit.Extract.TableType;
import gobblin.source.workunit.MultiWorkUnit;
import gobblin.source.workunit.WorkUnit;
import gobblin.util.ConfigUtils;
import gobblin.util.DatasetFilterUtils;
import gobblin.util.PathUtils;
import gobblin.util.dataset.DatasetUtils;
import lombok.extern.slf4j.Slf4j;


/**
 * A base implementation of {@link gobblin.source.Source} for
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

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    initLogger(state);

    List<WorkUnit> workUnits = Lists.newArrayList();
    String nameSpaceName = state.getProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY);

    Map<String, String> tableNameToEntityMap = Maps.newHashMap();
    Set<String> entities = getSourceEntities(state);
    List<Pattern> blacklist = DatasetFilterUtils.getPatternList(state, ENTITY_BLACKLIST);
    List<Pattern> whitelist = DatasetFilterUtils.getPatternList(state, ENTITY_WHITELIST);
    for (String entity : DatasetFilterUtils.filter(entities, blacklist, whitelist)) {
      tableNameToEntityMap.put(Utils.escapeSpecialCharacters(entity, ConfigurationKeys.ESCAPE_CHARS_IN_TABLE_NAME, "_"),
          entity);
    }
    
    Map<String, State> tableSpecificPropsMap = shouldObtainTablePropsFromConfigStore(state)
        ? getTableSpecificPropsFromConfigStore(tableNameToEntityMap.keySet(), state)
        : DatasetUtils.getDatasetSpecificProps(tableNameToEntityMap.keySet(), state);
    Map<String, Long> prevWatermarksByTable = getPreviousWatermarksForAllTables(state);

    for (String tableName : Sets.union(tableNameToEntityMap.keySet(), prevWatermarksByTable.keySet())) {

      log.info(String.format("Table to be processed: %s, %s", tableName,
          tableNameToEntityMap.containsKey(tableName) ? "source entity = " + tableNameToEntityMap.get(tableName)
              : "which does not have source entity but has previous watermark"));

      SourceState combinedState = getCombinedState(state, tableSpecificPropsMap.get(tableName));
      TableType tableType =
          TableType.valueOf(combinedState.getProp(ConfigurationKeys.EXTRACT_TABLE_TYPE_KEY).toUpperCase());

      long previousWatermark = prevWatermarksByTable.containsKey(tableName) ? prevWatermarksByTable.get(tableName)
          : ConfigurationKeys.DEFAULT_WATERMARK_VALUE;

      // If a table name exists in prevWatermarksByTable (i.e., it has a previous watermark) but does not exist
      // in talbeNameToEntityMap, create an empty workunit for it, so that its previous watermark is preserved.
      // This is done by overriding the high watermark to be the same as the previous watermark.
      if (!tableNameToEntityMap.containsKey(tableName)) {
        combinedState.setProp(ConfigurationKeys.SOURCE_QUERYBASED_END_VALUE, previousWatermark);
      }

      Map<Long, Long> sortedPartitions = Maps.newTreeMap();
      sortedPartitions.putAll(new Partitioner(combinedState).getPartitions(previousWatermark));

      Extract extract = createExtract(tableType, nameSpaceName, tableName);

      // Setting current time for the full extract
      if (Boolean.valueOf(combinedState.getProp(ConfigurationKeys.EXTRACT_IS_FULL_KEY))) {
        extract.setFullTrue(System.currentTimeMillis());
      }

      for (Entry<Long, Long> entry : sortedPartitions.entrySet()) {
        WorkUnit workunit = WorkUnit.create(extract);
        workunit.setProp(ConfigurationKeys.SOURCE_ENTITY, tableName);
        workunit.setWatermarkInterval(
            new WatermarkInterval(new LongWatermark(entry.getKey()), new LongWatermark(entry.getValue())));
        workUnits.add(workunit);
      }
    }
    log.info("Total number of workunits for the current run: " + workUnits.size());

    List<WorkUnit> previousWorkUnits = this.getPreviousWorkUnitsForRetry(state);
    log.info("Total number of incomplete tasks from the previous run: " + previousWorkUnits.size());
    workUnits.addAll(previousWorkUnits);

    int numOfMultiWorkunits =
        state.getPropAsInt(ConfigurationKeys.MR_JOB_MAX_MAPPERS_KEY, ConfigurationKeys.DEFAULT_MR_JOB_MAX_MAPPERS);

    return pack(workUnits, numOfMultiWorkunits);
  }

  protected Set<String> getSourceEntities(State state) {
    if (state.contains(ConfigurationKeys.SOURCE_ENTITIES)) {
      log.info("Using entity names in " + ConfigurationKeys.SOURCE_ENTITIES);
      return state.getPropAsSet(ConfigurationKeys.SOURCE_ENTITIES);
    } else if (state.contains(ConfigurationKeys.SOURCE_ENTITY)) {
      log.info("Using entity name in " + ConfigurationKeys.SOURCE_ENTITY);
      return ImmutableSet.of(state.getProp(ConfigurationKeys.SOURCE_ENTITY));
    }

    throw new IllegalStateException(String.format("One of the following properties must be specified: %s, %s.",
        ConfigurationKeys.SOURCE_ENTITIES, ConfigurationKeys.SOURCE_ENTITY));
  }

  private static boolean shouldObtainTablePropsFromConfigStore(SourceState state) {
    return state.getPropAsBoolean(SOURCE_OBTAIN_TABLE_PROPS_FROM_CONFIG_STORE,
        DEFAULT_SOURCE_OBTAIN_TABLE_PROPS_FROM_CONFIG_STORE);
  }

  private static Map<String, State> getTableSpecificPropsFromConfigStore(Set<String> tables, State state) {
    ConfigClient client = ConfigClientCache.getClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY);
    String configStoreUri = state.getProp(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI);
    Preconditions.checkNotNull(configStoreUri);

    Map<String, State> result = Maps.newHashMap();

    for (String table : tables) {
      try {
        result.put(table, ConfigUtils.configToState(
            client.getConfig(PathUtils.combinePaths(configStoreUri, QUERY_BASED_SOURCE, table).toUri())));
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
  private static Map<String, Long> getPreviousWatermarksForAllTables(SourceState state) {
    Map<String, Long> result = Maps.newHashMap();
    Map<String, Long> prevLowWatermarksByTable = Maps.newHashMap();
    Map<String, Long> prevActualHighWatermarksByTable = Maps.newHashMap();
    Set<String> tablesWithFailedTasks = Sets.newHashSet();
    boolean commitOnFullSuccess = JobCommitPolicy.getCommitPolicy(state) == JobCommitPolicy.COMMIT_ON_FULL_SUCCESS;

    for (WorkUnitState previousWus : state.getPreviousWorkUnitStates()) {
      
      String table = previousWus.getExtract().getTable();

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
    }

    for (Map.Entry<String, Long> entry : prevLowWatermarksByTable.entrySet()) {
      result.put(entry.getKey(), tablesWithFailedTasks.contains(entry.getKey()) ? entry.getValue()
          : prevActualHighWatermarksByTable.get(entry.getKey()));
    }

    return result;
  }

  private static boolean isSuccessfulOrCommited(WorkUnitState wus) {
    return wus.getWorkingState() == WorkingState.SUCCESSFUL || wus.getWorkingState() == WorkingState.COMMITTED;
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
