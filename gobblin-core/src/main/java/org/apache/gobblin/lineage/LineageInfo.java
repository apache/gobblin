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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * A class to restore all lineage information from a {@link State}
 * All lineage attributes are under LINEAGE_NAME_SPACE namespace.
 *
 * For example:
 *    gobblin.lineage.K1          ---> V1
 *    gobblin.lineage.branch.3.K2 ---> V2
 *
 * K1 is dataset level attribute, K2 is branch level attribute.
 */

@Slf4j
public class LineageInfo {

  public static final String LINEAGE_NAME_SPACE = "gobblin.lineage";
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

  private LineageInfo(String datasetUrn, String jobId, Map<String, String> lineageMetaData) {
    Preconditions.checkArgument(datasetUrn != null);
    Preconditions.checkArgument(jobId != null);
    this.datasetUrn = datasetUrn;
    this.jobId = jobId;
    this.lineageMetaData = lineageMetaData;
  }

  /**
   * Retrieve lineage information from a {@link State} by {@link Level}
   * @param level {@link Level#DATASET}  only load dataset level lineage attributes
   *              {@link Level#BRANCH}   only load branch level lineage attributes
   *              {@link Level#All}      load all lineage attributes
   * @return A {@link LineageInfo} object containing all lineage attributes
   */
  public static LineageInfo load (State state, Level level) throws LineageException {
    Preconditions.checkArgument(state != null);
    HashMap<String, String> metaData = new HashMap<>();

    for (Map.Entry<Object, Object> entry : state.getProperties().entrySet()) {
      if (entry.getKey() instanceof String && ((String) entry.getKey()).startsWith(LINEAGE_NAME_SPACE)) {

        String lineageKey = ((String) entry.getKey());
        String lineageValue = (String) entry.getValue();

        if (lineageKey.startsWith(BRANCH_PREFIX)) {
          if (level == Level.BRANCH || level == Level.All) {
            metaData.put(lineageKey.substring(BRANCH_PREFIX.length()), lineageValue);
          }
        } else if (lineageKey.startsWith(DATASET_PREFIX)) {
          if (level == Level.DATASET || level == Level.All) {
            metaData.put(lineageKey.substring(DATASET_PREFIX.length()), lineageValue);
          }
        }
      }
    }

    String jobId = metaData.containsKey(ConfigurationKeys.JOB_ID_KEY)? metaData.get(ConfigurationKeys.JOB_ID_KEY):"";
    String urn = metaData.containsKey(ConfigurationKeys.DATASET_URN_KEY)? metaData.get(ConfigurationKeys.DATASET_URN_KEY):ConfigurationKeys.DEFAULT_DATASET_URN;

    metaData.remove(ConfigurationKeys.JOB_ID_KEY);
    metaData.remove(ConfigurationKeys.DATASET_URN_KEY);

    LineageInfo descriptor = new LineageInfo(urn, jobId, metaData);

    return descriptor;
  }

  /**
   * Get all lineage meta data.
   */
  public ImmutableMap<String, String> getLineageMetaData() {
    return ImmutableMap.copyOf(lineageMetaData);
  }


  public static LineageInfo load (Collection<? extends State> states, Level level) throws LineageException {
    Preconditions.checkArgument(states != null && !states.isEmpty());
    HashMap<String, String> metaData = new HashMap<>();

    for (State state: states) {
      for (Map.Entry<Object, Object> entry : state.getProperties().entrySet()) {
        if (entry.getKey() instanceof String && ((String) entry.getKey()).startsWith(LINEAGE_NAME_SPACE)) {

          String lineageKey = ((String) entry.getKey());
          String lineageValue = (String) entry.getValue();

          if (lineageKey.startsWith(BRANCH_PREFIX)) {
            if (level == Level.BRANCH || level == Level.All) {
              String prev = metaData.put(lineageKey.substring(BRANCH_PREFIX.length()), lineageValue);
              if (prev != null && !prev.equals(lineageValue)) {
                throw new LineageException.LineageConflictAttributeException(lineageKey, prev, lineageValue);
              }
            }
          } else if (lineageKey.startsWith(DATASET_PREFIX)) {
            if (level == Level.DATASET || level == Level.All) {
              String prev = metaData.put(lineageKey.substring(DATASET_PREFIX.length()), lineageValue);
              if (prev != null && !prev.equals(lineageValue)) {
                throw new LineageException.LineageConflictAttributeException(lineageKey, prev, lineageValue);
              }
            }
          }
        }
      }
    }

    String jobId = metaData.containsKey(ConfigurationKeys.JOB_ID_KEY)? metaData.get(ConfigurationKeys.JOB_ID_KEY):"";
    String urn = metaData.containsKey(ConfigurationKeys.DATASET_URN_KEY)? metaData.get(ConfigurationKeys.DATASET_URN_KEY):ConfigurationKeys.DEFAULT_DATASET_URN;

    metaData.remove(ConfigurationKeys.JOB_ID_KEY);
    metaData.remove(ConfigurationKeys.DATASET_URN_KEY);

    LineageInfo descriptor = new LineageInfo(urn, jobId, metaData);

    return descriptor;
  }

  public static void setDatasetLineageAttribute (State state, String key, String value) {
    state.setProp(DATASET_PREFIX + key, value);
  }

  public static void setBranchLineageAttribute (State state, int branchId, String key, String value) {
    state.setProp(BRANCH_PREFIX + Joiner.on(".").join(branchId, key), value);
  }

  public final String getUUID() {
    return Joiner.on(":::").join(this.datasetUrn, this.jobId);
  }
}
