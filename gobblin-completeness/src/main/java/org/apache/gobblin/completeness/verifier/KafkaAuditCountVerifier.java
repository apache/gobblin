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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
  public static final String TOTAL_COUNT_REFERENCE_TIERS = COMPLETENESS_PREFIX + "totalCount.reference.tiers";
  public static final String THRESHOLD = COMPLETENESS_PREFIX + "threshold";
  private static final double DEFAULT_THRESHOLD = 0.999;
  public static final String COMPLETE_ON_NO_COUNTS = COMPLETENESS_PREFIX + "complete.on.no.counts";

  public enum CompletenessType {
    ClassicCompleteness,
    TotalCountCompleteness
  }

  private final boolean returnCompleteOnNoCounts;

  private final AuditCountClient auditCountClient;
  private final String srcTier;
  private final Collection<String> refTiers;
  private final Collection<String> totalCountRefTiers;
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
    this.totalCountRefTiers = state.contains(TOTAL_COUNT_REFERENCE_TIERS)
        ? Splitter.on(",").omitEmptyStrings().trimResults().splitToList(state.getProp(TOTAL_COUNT_REFERENCE_TIERS))
        : null;
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

  public Map<CompletenessType, Boolean> calculateCompleteness(String datasetName, long beginInMillis, long endInMillis)
      throws IOException {
    return calculateCompleteness(datasetName, beginInMillis, endInMillis, this.threshold);
  }

  /**
   * Compare source tier against reference tiers.
   * Compute completion percentage which is true iff the calculated percentages is greater than threshold.
   *
   * @param datasetName A dataset short name like 'PageViewEvent'
   * @param beginInMillis Unix timestamp in milliseconds
   * @param endInMillis Unix timestamp in milliseconds
   * @param threshold User defined threshold
   *
   * @return a map of completeness result by CompletenessType
   */
  public Map<CompletenessType, Boolean> calculateCompleteness(String datasetName, long beginInMillis, long endInMillis,
      double threshold) throws IOException {
    Map<String, Long> countsByTier = getTierAndCount(datasetName, beginInMillis, endInMillis);
    log.info(String.format("checkTierCounts: audit counts map for %s for range [%s,%s]", datasetName, beginInMillis, endInMillis));
    countsByTier.forEach((x,y) -> log.info(String.format(" %s : %s ", x, y)));

    Map<CompletenessType, Boolean> result = new HashMap<>();
    Arrays.stream(CompletenessType.values()).forEach(type -> {
      try {
        result.put(type, calculateCompleteness(datasetName, beginInMillis, endInMillis, type, countsByTier) > threshold);
      } catch (IOException e) {
        log.error("Failed to calculate completeness for type " + type, e);
      }
    });
    return result;
  }

  private double calculateCompleteness(String datasetName, long beginInMillis, long endInMillis, CompletenessType type,
      Map<String, Long> countsByTier) throws IOException {
    if (countsByTier.isEmpty() && this.returnCompleteOnNoCounts) {
      log.info(String.format("Found empty counts map for %s, returning complete", datasetName));
      return 1.0;
    }

    switch (type) {
      case ClassicCompleteness:
        return calculateClassicCompleteness(datasetName, beginInMillis, endInMillis, countsByTier);
      case TotalCountCompleteness:
        return calculateTotalCountCompleteness(datasetName, beginInMillis, endInMillis, countsByTier);
      default:
        log.error("Skip unsupported completeness type {}", type);
        return -1;
    }
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
  private double calculateClassicCompleteness(String datasetName, long beginInMillis, long endInMillis,
      Map<String, Long> countsByTier) throws IOException {
    validateTierCounts(datasetName, beginInMillis, endInMillis, countsByTier, this.srcTier, this.refTiers);

    double percent = -1;
    for (String refTier: this.refTiers) {
      long refCount = countsByTier.get(refTier);
      long srcCount = countsByTier.get(this.srcTier);

      /*
        If we have a case where an audit map is returned, however, one of the source tiers on another fabric is 0,
        and the reference tiers from Kafka is reported to be 0, we can say that this hour is complete.
        This needs to be added as a non-zero double value divided by 0 is infinity, but 0 divided by 0 is NaN.
       */
      if (srcCount == 0 && refCount == 0) {
        return 1.0;
      }
      percent = Double.max(percent, (double) srcCount / (double) refCount);
    }

    if (percent < 0) {
      throw new IOException("Cannot calculate completion percentage");
    }
    return percent;
  }

  /**
   * Check total count based completeness by comparing source tier against reference tiers,
   * and calculate the completion percentage by srcCount/sum_of(refCount).
   *
   * @param datasetName A dataset short name like 'PageViewEvent'
   * @param beginInMillis Unix timestamp in milliseconds
   * @param endInMillis Unix timestamp in milliseconds
   *
   * @return The percentage value by srcCount/sum_of(refCount)
   */
  private double calculateTotalCountCompleteness(String datasetName, long beginInMillis, long endInMillis,
      Map<String, Long> countsByTier) throws IOException {
    if (this.totalCountRefTiers == null) {
      return -1;
    }
    validateTierCounts(datasetName, beginInMillis, endInMillis, countsByTier, this.srcTier, this.totalCountRefTiers);

    long srcCount = countsByTier.get(this.srcTier);
    long totalRefCount = this.totalCountRefTiers
        .stream()
        .mapToLong(countsByTier::get)
        .sum();
    /*
      If we have a case where an audit map is returned, however, one of the source tiers on another fabric is 0,
      and the sum of the reference tiers from Kafka is reported to be 0, we can say that this hour is complete.
      This needs to be added as a non-zero double value divided by 0 is infinity, but 0 divided by 0 is NaN.
     */
    if (srcCount == 0 && totalRefCount == 0) {
      return 1.0;
    }
    double percent = Double.max(-1, (double) srcCount / (double) totalRefCount);
    if (percent < 0) {
      throw new IOException("Cannot calculate total count completion percentage");
    }
    return percent;
  }

  private static void validateTierCounts(String datasetName, long beginInMillis, long endInMillis, Map<String, Long> countsByTier,
      String sourceTier, Collection<String> referenceTiers)
      throws IOException {
    if (!countsByTier.containsKey(sourceTier)) {
      throw new IOException(String.format("Source tier %s audit count cannot be retrieved for dataset %s between %s and %s", sourceTier, datasetName, beginInMillis, endInMillis));
    }

    for (String refTier: referenceTiers) {
      if (!countsByTier.containsKey(refTier)) {
        throw new IOException(String.format("Reference tier %s audit count cannot be retrieved for dataset %s between %s and %s", refTier, datasetName, beginInMillis, endInMillis));
      }
      long refCount = countsByTier.get(refTier);
      if (refCount == 0) {
        // If count in refTier is 0, it will be assumed that the data for that hour is completed and move the watermark forward.
        log.warn(String.format("Reference tier %s audit count is reported to be zero", refCount));
      } else if (refCount < 0) {
        throw new IOException(String.format("Reference tier %s count cannot be less than zero", refTier));
      }
    }
  }

  /**
   * Fetch all <tier-count> pairs for a given dataset between a time range
   */
  private Map<String, Long> getTierAndCount(String datasetName, long beginInMillis, long endInMillis) throws IOException {
    return auditCountClient.fetch(datasetName, beginInMillis, endInMillis);
  }
}