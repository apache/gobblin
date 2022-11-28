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

package org.apache.gobblin.service.monitoring;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.text.StringEscapeUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.metrics.RuntimeMetrics;
import org.apache.gobblin.runtime.spec_catalog.AddSpecResponse;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;


/**
 * A Flow Spec Store change monitor that uses {@link GenericStoreChangeEvent} schema to process Kafka messages received
 * from the consumer service. This monitor responds to changes to flow specs (creations, updates, deletes) and acts as
 * a connector between the API and execution layers of GaaS.
 */
@Slf4j
public class SpecStoreChangeMonitor extends HighLevelConsumer {
  public static final String SPEC_STORE_CHANGE_MONITOR_PREFIX = "specStoreChangeMonitor";

  // Metrics
  private ContextAwareMeter successfullyAddedSpecs;
  private ContextAwareMeter messageProcessedMeter;
  private ContextAwareMeter failedAddedSpecs;
  private ContextAwareMeter deletedSpecs;
  private ContextAwareMeter unexpectedErrors;

  protected CacheLoader<String, String> cacheLoader = new CacheLoader<String, String>() {
    @Override
    public String load(String key) throws Exception {
      return key;
    }
  };

  protected LoadingCache<String, String>
      specChangesSeenCache = CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(cacheLoader);

  @Inject
  protected FlowCatalog flowCatalog;

  @Inject
  protected GobblinServiceJobScheduler scheduler;

  // Note that the topic is an empty string (rather than null to avoid NPE) because this monitor relies on the consumer
  // client itself to determine all Kafka related information dynamically rather than through the config.
  public SpecStoreChangeMonitor(String topic, Config config, int numThreads) {
    // Differentiate group id for each host
    super(topic, config.withValue(GROUP_ID_KEY,
        ConfigValueFactory.fromAnyRef(SPEC_STORE_CHANGE_MONITOR_PREFIX + UUID.randomUUID().toString())),
        numThreads);
  }

  @Override
  protected void assignTopicPartitions() {
    // Expects underlying consumer to handle initializing partitions and offset for the topic -
    // subscribe to all partitions from latest offset
    return;
  }

  @Override
  /*
  Note that although this class is multi-threaded and will call this message for multiple threads (each having a queue
  associated with it), a given message itself will be partitioned and assigned to only one queue.
   */
  protected void processMessage(DecodeableKafkaRecord message) {
    // This will also include the heathCheck message so that we can rely on this to monitor the health of this Monitor
    messageProcessedMeter.mark();
    String key = (String) message.getKey();
    GenericStoreChangeEvent value = (GenericStoreChangeEvent) message.getValue();

    Long timestamp = value.getTimestamp();
    String operation = value.getOperationType().name();

    log.debug("Processing message where specUri is {} timestamp is {} operation is {}", key, timestamp, operation);

    String changeIdentifier = timestamp + key;
    if (!ChangeMonitorUtils.shouldProcessMessage(changeIdentifier, specChangesSeenCache, operation,
        timestamp.toString())) {
      return;
    }

    Spec spec;
    URI specAsUri = null;

    try {
      specAsUri = new URI(key);
    } catch (URISyntaxException e) {
      log.warn("Could not create URI object for specUri {} due to error {}", key, e.getMessage());
      this.unexpectedErrors.mark();
      return;
    }

    spec = (!operation.equals("DELETE")) ? this.flowCatalog.getSpecWrapper(specAsUri) : null;

    // The monitor should continue to process messages regardless of failures with individual messages, instead we use
    // metrics to keep track of failure to process certain SpecStoreChange events
    try {
      // Call respective action for the type of change received
      AddSpecResponse response;
      if (operation.equals("INSERT") || operation.equals("UPDATE")) {
        response = scheduler.onAddSpec(spec);

        // Null response means the dag failed to compile
        if (response != null && FlowCatalog.isCompileSuccessful((String) response.getValue())) {
          log.info("Successfully added spec {} to scheduler response {}", spec,
              StringEscapeUtils.escapeJson(response.getValue().toString()));
          this.successfullyAddedSpecs.mark();
        } else {
          log.warn("Failed to add spec {} to scheduler due to compile error. The flow graph changed recently to "
              + "invalidate the earlier compilation. Examine changes to locate error. Response is {}", spec, response);
          this.failedAddedSpecs.mark();
        }
      } else if (operation.equals("DELETE")) {
        scheduler.onDeleteSpec(specAsUri, FlowSpec.Builder.DEFAULT_VERSION);
        this.deletedSpecs.mark();
      } else {
        log.warn("Received unsupported change type of operation {}. Expected values to be in "
                + "[INSERT, UPDATE, DELETE, HEARTBEAT]. Look for issue with kafka event consumer or emitter",
            operation);
        this.unexpectedErrors.mark();
        return;
      }
    } catch (Exception e) {
      log.warn("Ran into unexpected error processing SpecStore changes. Reexamine scheduler. Error: {}", e);
      this.unexpectedErrors.mark();
    }

    specChangesSeenCache.put(changeIdentifier, changeIdentifier);
  }

 @Override
  protected void createMetrics() {
    super.createMetrics();
    this.successfullyAddedSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_SPEC_STORE_MONITOR_SUCCESSFULLY_ADDED_SPECS);
    this.failedAddedSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_SPEC_STORE_MONITOR_FAILED_ADDED_SPECS);
    this.deletedSpecs = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_SPEC_STORE_MONITOR_DELETED_SPECS);
    this.unexpectedErrors = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_SPEC_STORE_MONITOR_UNEXPECTED_ERRORS);
    this.messageProcessedMeter = this.getMetricContext().contextAwareMeter(RuntimeMetrics.GOBBLIN_SPEC_STORE_MESSAGE_PROCESSED);
  }
}