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

package org.apache.gobblin.compaction.verify;

import com.google.common.base.Splitter;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.gobblin.compaction.audit.AuditCountClient;
import org.apache.gobblin.compaction.audit.AuditCountClientFactory;
import org.apache.gobblin.compaction.mapreduce.MRCompactor;
import org.apache.gobblin.compaction.parser.CompactionPathParser;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.dataset.FileSystemDataset;
import org.apache.gobblin.util.ClassAliasResolver;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * Use {@link AuditCountClient} to retrieve all record count across different tiers
 * Compare one specific tier (gobblin-tier) with all other refernce tiers and determine
 * if verification should be passed based on a pre-defined threshold.
 */
@Slf4j
public class CompactionAuditCountVerifier implements CompactionVerifier<FileSystemDataset> {

  public static final String COMPACTION_COMPLETENESS_THRESHOLD = MRCompactor.COMPACTION_PREFIX + "completeness.threshold";
  public static final double DEFAULT_COMPACTION_COMPLETENESS_THRESHOLD = 0.99;
  public static final String PRODUCER_TIER = "producer.tier";
  public static final String ORIGIN_TIER = "origin.tier";
  public static final String GOBBLIN_TIER = "gobblin.tier";

  private  Collection<String> referenceTiers;
  private  Collection<String> originTiers;
  private  String producerTier;
  private  String gobblinTier;
  private  double threshold;
  private final State state;
  private final AuditCountClient auditCountClient;

  /**
   * Constructor with default audit count client
   */
  public CompactionAuditCountVerifier (State state) {
    this (state, getClientFactory (state).createAuditCountClient(state));
  }

  /**
   * Constructor with user specified audit count client
   */
  public CompactionAuditCountVerifier (State state, AuditCountClient client) {
    this.auditCountClient = client;
    this.state = state;

    // retrieve all tiers information
    if (client != null) {
      this.threshold =
              state.getPropAsDouble(COMPACTION_COMPLETENESS_THRESHOLD, DEFAULT_COMPACTION_COMPLETENESS_THRESHOLD);
      this.producerTier = state.getProp(PRODUCER_TIER);
      this.gobblinTier = state.getProp(GOBBLIN_TIER);
      this.originTiers = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(state.getProp(ORIGIN_TIER));
      this.referenceTiers = new HashSet<>(originTiers);
      this.referenceTiers.add(producerTier);
    }
  }

  /**
   * Obtain a client factory
   * @param state job state
   * @return a factory which creates {@link AuditCountClient}.
   *         If no factory is set or an error occurred, a {@link EmptyAuditCountClientFactory} is
   *         returned which creates a <code>null</code> {@link AuditCountClient}
   */
  private static AuditCountClientFactory getClientFactory (State state) {

    if (!state.contains(AuditCountClientFactory.AUDIT_COUNT_CLIENT_FACTORY)) {
      return new EmptyAuditCountClientFactory ();
    }

    try {
      String factoryName = state.getProp(AuditCountClientFactory.AUDIT_COUNT_CLIENT_FACTORY);
      ClassAliasResolver<AuditCountClientFactory> conditionClassAliasResolver = new ClassAliasResolver<>(AuditCountClientFactory.class);
      AuditCountClientFactory factory = conditionClassAliasResolver.resolveClass(factoryName).newInstance();
      return factory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Verify a specific dataset by following below steps
   *    1) Retrieve a tier-to-count mapping
   *    2) Read count from {@link CompactionAuditCountVerifier#gobblinTier}
   *    3) Read count from all other {@link CompactionAuditCountVerifier#referenceTiers}
   *    4) Compare count retrieved from steps 2) and 3), if any of (gobblin/refenence) >= threshold, return true, else return false
   * @param dataset Dataset needs to be verified
   * @return If verification is succeeded
   */
  public Result verify (FileSystemDataset dataset) {
    if (auditCountClient == null) {
      log.debug("No audit count client specified, skipped");
      return new Result(true, "");
    }

    CompactionPathParser.CompactionParserResult result = new CompactionPathParser(this.state).parse(dataset);
    DateTime startTime = result.getTime();
    DateTime endTime = startTime.plusHours(1);
    String datasetName = result.getDatasetName();
    try {
      Map<String, Long> countsByTier = auditCountClient.fetch (datasetName, startTime.getMillis(), endTime.getMillis());
      for (String tier: referenceTiers) {
        Result rst = passed (datasetName, countsByTier, tier);
        if (rst.isSuccessful()) {
          return new Result(true, "");
        }
      }
    } catch (IOException e) {
      return new Result(false, ExceptionUtils.getFullStackTrace(e));
    }

    return new Result(false, String.format("%s data is not complete between %s and %s", datasetName, startTime, endTime));
  }

  /**
   * Compare record count between {@link CompactionAuditCountVerifier#gobblinTier} and {@link CompactionAuditCountVerifier#referenceTiers}.
   * @param datasetName the name of dataset
   * @param countsByTier the tier-to-count mapping retrieved by {@link AuditCountClient#fetch(String, long, long)}
   * @param referenceTier the tiers we wants to compare against
   * @return If any of (gobblin/refenence) >= threshold, return true, else return false
   */
  private Result passed (String datasetName, Map<String, Long> countsByTier, String referenceTier) {
    if (!countsByTier.containsKey(this.gobblinTier)) {
      return new Result(false, String.format("Failed to get audit count for topic %s, tier %s", datasetName, this.gobblinTier));
    }
    if (!countsByTier.containsKey(referenceTier)) {
      return new Result(false, String.format("Failed to get audit count for topic %s, tier %s", datasetName, referenceTier));
    }

    long refCount = countsByTier.get(referenceTier);
    long gobblinCount = countsByTier.get(this.gobblinTier);

    if ((double) gobblinCount / (double) refCount < this.threshold) {
      return new Result (false, String.format("%s failed for %s : gobblin count = %d, %s count = %d (%f < threshold %f)",
              this.getName(), datasetName, gobblinCount, referenceTier, refCount, (double) gobblinCount / (double) refCount, this.threshold));
    }

    return new Result(true, "");
  }

  public String getName() {
    return this.getClass().getName();
  }

  private static class EmptyAuditCountClientFactory implements AuditCountClientFactory {
    public AuditCountClient createAuditCountClient (State state) {
      return null;
    }
  }

  public boolean isRetriable () {
    return true;
  }
}
