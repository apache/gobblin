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

package org.apache.gobblin.completeness.verifier;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.completeness.audit.AuditCountClient;
import org.apache.gobblin.completeness.audit.AuditCountClientFactory;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.util.ClassAliasResolver;


/**
 * Use {@link AuditCountClient} to retrieve all record count across different tiers
 * Compare one source tier against all other reference tiers and determine
 * if verification should be passed based on a pre-defined threshold.
 * source tier is the tier being compared against single/multiple reference tiers
 */
@Slf4j
public class KafkaAuditCountVerifier {
  public static final String COMPLETENESS_PREFIX = "completeness.";
  public static final String SOURCE_TIER = COMPLETENESS_PREFIX + "source.tier";
  public static final String REFERENCE_TIERS = COMPLETENESS_PREFIX + "reference.tiers";
  public static final String THRESHOLD = COMPLETENESS_PREFIX + "threshold";
  private static final double DEFAULT_THRESHOLD = 0.999;
  public static final String COMPLETE_ON_NO_COUNTS = COMPLETENESS_PREFIX + "complete.on.no.counts";
  private final boolean returnCompleteOnNoCounts;

  private final AuditCountClient auditCountClient;
  private final String srcTier;
  private final Collection<String> refTiers;
  private final double threshold;

  /**
   * Constructor with audit count client from state
   */
  public KafkaAuditCountVerifier(State state) {
    this(state, getAuditClient(state));
  }

  /**
   * Constructor with user specified audit count client
   */
  public KafkaAuditCountVerifier(State state, AuditCountClient client) {
    this.auditCountClient = client;
    this.threshold =
        state.getPropAsDouble(THRESHOLD, DEFAULT_THRESHOLD);
    this.srcTier = state.getProp(SOURCE_TIER);
    this.refTiers = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(state.getProp(REFERENCE_TIERS));
    this.returnCompleteOnNoCounts = state.getPropAsBoolean(COMPLETE_ON_NO_COUNTS, false);
  }

  /**
   * Obtain an {@link AuditCountClient} using a {@link AuditCountClientFactory}
   * @param state job state
   * @return {@link AuditCountClient}
   */
  private static AuditCountClient getAuditClient(State state) {
    Preconditions.checkArgument(state.contains(AuditCountClientFactory.AUDIT_COUNT_CLIENT_FACTORY),
        String.format("Audit count factory %s not set ", AuditCountClientFactory.AUDIT_COUNT_CLIENT_FACTORY));
    try {
      String factoryName = state.getProp(AuditCountClientFactory.AUDIT_COUNT_CLIENT_FACTORY);
      ClassAliasResolver<AuditCountClientFactory> conditionClassAliasResolver = new ClassAliasResolver<>(AuditCountClientFactory.class);
      AuditCountClientFactory factory = conditionClassAliasResolver.resolveClass(factoryName).newInstance();
      return factory.createAuditCountClient(state);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Compare source tier against reference tiers.
   * Compute completion percentage by srcCount/refCount. Return true iff the highest percentages is greater than threshold.
   *
   * @param datasetName A dataset short name like 'PageViewEvent'
   * @param beginInMillis Unix timestamp in milliseconds
   * @param endInMillis Unix timestamp in milliseconds
   * @param threshold User defined threshold
   */
  public boolean isComplete(String datasetName, long beginInMillis, long endInMillis, double threshold)
      throws IOException {
    return getCompletenessPercentage(datasetName, beginInMillis, endInMillis) > threshold;
  }

  public boolean isComplete(String datasetName, long beginInMillis, long endInMillis)
      throws IOException {
    return isComplete(datasetName, beginInMillis, endInMillis, this.threshold);
  }

  /**
   * Compare source tier against reference tiers. For each reference tier, calculates percentage by srcCount/refCount.
   *
   * @param datasetName A dataset short name like 'PageViewEvent'
   * @param beginInMillis Unix timestamp in milliseconds
   * @param endInMillis Unix timestamp in milliseconds
   *
   * @return The highest percentage value
   */
  private double getCompletenessPercentage(String datasetName, long beginInMillis, long endInMillis) throws IOException {
    Map<String, Long> countsByTier = getTierAndCount(datasetName, beginInMillis, endInMillis);
    log.info(String.format("Audit counts map for %s for range [%s,%s]", datasetName, beginInMillis, endInMillis));
    countsByTier.forEach((x,y) -> log.info(String.format(" %s : %s ", x, y)));
    if (countsByTier.isEmpty() && this.returnCompleteOnNoCounts) {
      log.info(String.format("Found empty counts map for %s, returning complete", datasetName));
      return 1.0;
    }
    double percent = -1;
    if (!countsByTier.containsKey(this.srcTier)) {
      throw new IOException(String.format("Source tier %s audit count cannot be retrieved for dataset %s between %s and %s", this.srcTier, datasetName, beginInMillis, endInMillis));
    }

    for (String refTier: this.refTiers) {
      if (!countsByTier.containsKey(refTier)) {
        throw new IOException(String.format("Reference tier %s audit count cannot be retrieved for dataset %s between %s and %s", refTier, datasetName, beginInMillis, endInMillis));
      }
      long refCount = countsByTier.get(refTier);
      if(refCount <= 0) {
        throw new IOException(String.format("Reference tier %s count cannot be less than or equal to zero", refTier));
      }
      long srcCount = countsByTier.get(this.srcTier);

      percent = Double.max(percent, (double) srcCount / (double) refCount);
    }

    if (percent < 0) {
      throw new IOException("Cannot calculate completion percentage");
    }

    return percent;
  }

  /**
   * Fetch all <tier-count> pairs for a given dataset between a time range
   */
  private Map<String, Long> getTierAndCount(String datasetName, long beginInMillis, long endInMillis) throws IOException {
    return auditCountClient.fetch(datasetName, beginInMillis, endInMillis);
  }
}