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

package org.apache.gobblin.lineage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A class to restore all lineage information from a {@link State}
 * All lineage attributes are under LINEAGE_NAME_SPACE namespace.
 *
 * For example, a typical lineage attributes looks like:
 *    gobblin.lineage.K1          ---> V1
 *    gobblin.lineage.branch.3.K2 ---> V2
 *
 * K1 is dataset level attribute, K2 is branch level attribute, and branch id is 3.
 */

@Slf4j
public class LineageInfo {
  public static final String LINEAGE_DATASET_URN = "lineage.dataset.urn";
  public static final String LINEAGE_NAME_SPACE = "gobblin.lineage";
  public static final String BRANCH_ID_METADATA_KEY = "branchId";
  private static final String DATASET_PREFIX =  LINEAGE_NAME_SPACE + ".";
  private static final String BRANCH_PREFIX = DATASET_PREFIX + "branch.";

  @Getter
  private String datasetUrn;
  @Getter
  private String jobId;

  private Map<String, String> lineageMetaData;

  public enum Level {
    DATASET,
    BRANCH,
    All
  }

  private LineageInfo() {
  }

  private LineageInfo(String datasetUrn, String jobId, ImmutableMap<String, String> lineageMetaData) {
    Preconditions.checkArgument(datasetUrn != null);
    Preconditions.checkArgument(jobId != null);
    this.datasetUrn = datasetUrn;
    this.jobId = jobId;
    this.lineageMetaData = lineageMetaData;
  }

  /**
   * Retrieve lineage information from a {@link State} by {@link Level}
   * @param state A single state
   * @param level {@link Level#DATASET}  only load dataset level lineage attributes
   *              {@link Level#BRANCH}   only load branch level lineage attributes
   *              {@link Level#All}      load all lineage attributes
   * @return A collection of {@link LineageInfo}s per branch. When level is {@link Level#DATASET}, this list has only single element.
   */
  public static Collection<LineageInfo> load (State state, Level level) throws LineageException {
    return load(Collections.singleton(state), level);
  }

  /**
   * Get all lineage meta data.
   */
  public ImmutableMap<String, String> getLineageMetaData() {
    return ImmutableMap.copyOf(lineageMetaData);
  }

  /**
   * Retrieve all lineage information from different {@link State}s.
   * This requires the job id and dataset urn to be present in the state, under job.id and dataset.urn.
   * A global union operation is applied to combine all <K, V> pairs from the input {@link State}s. If multiple {@link State}s
   * share the same K, but have conflicting V, a {@link LineageException} is thrown.
   *
   * {@link Level} can control if a dataset level or branch level information should be used. When {@link Level#All} is
   * specified, all levels of information will be returned; otherwise only specified level of information will be returned.
   *
   * For instance, assume we have below input states:
   *    State[0]: gobblin.lineage.K1          ---> V1
   *              gobblin.lineage.K2          ---> V2
   *              gobblin.lineage.branch.1.K4 ---> V4
   *    State[1]: gobblin.lineage.K2          ---> V2
   *              gobblin.lineage.K3          ---> V3
   *              gobblin.lineage.branch.1.K4 ---> V4
   *              gobblin.lineage.branch.1.K5 ---> V5
   *              gobblin.lineage.branch.2.K6 ---> V6
   *
   *  (1) With {@link Level#DATASET} level, the output would be:
   *      LinieageInfo[0]:  K1 ---> V1
   *                        K2 ---> V2
   *                        K3 ---> V3
   *  (2) With {@link Level#All} level, the output would be: (because there are two branches, so there are two LineageInfo)
   *      LineageInfo[0]:   K1 ---> V1
   *                        K2 ---> V2
   *                        K3 ---> V3
   *                        K4 ---> V4
   *                        K5 ---> V5
   *
   *      LineageInfo[1]:   K1 ---> V1
   *                        K2 ---> V2
   *                        K3 ---> V3
   *                        K6 ---> V6
   *
   *   (3) With {@link Level#BRANCH} level, the output would be: (only branch level information was returned)
   *      LineageInfo[0]:   K4 ---> V4
   *                        K5 ---> V5
   *      LineageInfo[1]:   K6 ---> V6
   *
   * @param states All states which belong to the same dataset and share the same jobId.
   * @param level {@link Level#DATASET}  only load dataset level lineage attributes
   *              {@link Level#BRANCH}   only load branch level lineage attributes
   *              {@link Level#All}      load all lineage attributes
   * @return A collection of {@link LineageInfo}s per branch. When level is {@link Level#DATASET}, this list has only single element.
   *
   * @throws LineageException.LineageConflictAttributeException if two states have same key but not the same value.
   */
  public static Collection<LineageInfo> load (Collection<? extends State> states, Level level) throws LineageException {
    Preconditions.checkArgument(states != null && !states.isEmpty());
    Map<String, String> datasetMetaData = new HashMap<>();
    Map<String, Map<String, String>> branchAggregate = new HashMap<>();

    State anyOne = states.iterator().next();
    String jobId = anyOne.getProp(ConfigurationKeys.JOB_ID_KEY, "");
    String urn = anyOne.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN);

    for (State state: states) {
      for (Map.Entry<Object, Object> entry : state.getProperties().entrySet()) {
        if (entry.getKey() instanceof String && ((String) entry.getKey()).startsWith(LINEAGE_NAME_SPACE)) {

          String lineageKey = ((String) entry.getKey());
          String lineageValue = (String) entry.getValue();

          if (lineageKey.startsWith(BRANCH_PREFIX)) {
            String branchPrefixStrip = lineageKey.substring(BRANCH_PREFIX.length());
            String branchId = branchPrefixStrip.substring(0, branchPrefixStrip.indexOf("."));
            String key = branchPrefixStrip.substring(branchPrefixStrip.indexOf(".") + 1);

            if (level == Level.BRANCH || level == Level.All) {
              if (!branchAggregate.containsKey(branchId)) {
                branchAggregate.put(branchId, new HashMap<>());
              }
              Map<String, String> branchMetaData = branchAggregate.get(branchId);
              String prev = branchMetaData.put(key, lineageValue);
              if (prev != null && !prev.equals(lineageValue)) {
                throw new LineageException.LineageConflictAttributeException(lineageKey, prev, lineageValue);
              }
            }
          } else if (lineageKey.startsWith(DATASET_PREFIX)) {
            if (level == Level.DATASET || level == Level.All) {
              String prev = datasetMetaData.put(lineageKey.substring(DATASET_PREFIX.length()), lineageValue);
              if (prev != null && !prev.equals(lineageValue)) {
                throw new LineageException.LineageConflictAttributeException(lineageKey, prev, lineageValue);
              }
            }
          }
        }
      }
    }

    Collection<LineageInfo> collection = Sets.newHashSet();

    if (level == Level.DATASET) {
      ImmutableMap<String, String> metaData = ImmutableMap.<String, String>builder()
          .putAll(datasetMetaData)
          .build();
      collection.add(new LineageInfo(urn, jobId, metaData));
      return collection;
    } else if (level == Level.BRANCH || level == Level.All){
      if (branchAggregate.isEmpty()) {
        if (level == Level.All) {
          collection.add(new LineageInfo(urn, jobId, ImmutableMap.<String, String>builder().putAll(datasetMetaData).build()));
        }
        return collection;
      }
      for (Map.Entry<String, Map<String, String>> branchMetaDataEntry: branchAggregate.entrySet()) {
        String branchId = branchMetaDataEntry.getKey();
        Map<String, String> branchMetaData = branchMetaDataEntry.getValue();
        ImmutableMap.Builder<String, String> metaDataBuilder = ImmutableMap.builder();
        if (level == Level.All) {
          metaDataBuilder.putAll(datasetMetaData);
        }
        metaDataBuilder.putAll(branchMetaData).put(BRANCH_ID_METADATA_KEY, branchId);
        collection.add(new LineageInfo(urn, jobId, metaDataBuilder.build()));
      }

      return collection;
    } else {
      throw new LineageException.LineageUnsupportedLevelException(level);
    }
  }

  public static void setDatasetLineageAttribute (State state, String key, String value) {
    state.setProp(DATASET_PREFIX + key, value);
  }

  public static void setBranchLineageAttribute (State state, int branchId, String key, String value) {
    state.setProp(BRANCH_PREFIX + Joiner.on(".").join(branchId, key), value);
  }

  public static Map<String, Collection<State>> aggregateByDatasetUrn (Collection<? extends State> states) {
    Map<String, Collection<State>> datasetStates = new HashMap<>();
    for (State state: states) {
      String urn = state.getProp(LINEAGE_DATASET_URN, state.getProp(ConfigurationKeys.DATASET_URN_KEY, ConfigurationKeys.DEFAULT_DATASET_URN));
      datasetStates.putIfAbsent(urn, new ArrayList<>());
      Collection<State> datasetState = datasetStates.get(urn);
      datasetState.add(state);
    }
    return datasetStates;
  }

  public final String getId() {
    return Joiner.on(":::").join(this.datasetUrn, this.jobId);
  }
}
