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

import java.util.HashSet;

import com.google.inject.Inject;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.kafka.HighLevelConsumer;
import org.apache.gobblin.runtime.spec_catalog.FlowCatalog;
import org.apache.gobblin.service.modules.scheduler.GobblinServiceJobScheduler;


/**
 * A Flow Spec Store change monitor that uses {@link SpecStoreChangeEvent} schema to process Kafka messages received
 * from the consumer service. This monitor responds to changes to flow specs (creations, updates, deletes) and acts as
 * a connector between the API and execution layers of GaaS.
 */
@Slf4j
public class SpecStoreChangeMonitor extends HighLevelConsumer {
  protected HashSet<Long> timestampsSeenBefore;

  @Inject
  protected FlowCatalog flowCatalog;

  @Inject
  protected GobblinServiceJobScheduler scheduler;

  @Inject
  public SpecStoreChangeMonitor(String topic, Config config, int numThreads) {
    super(topic, config, numThreads);
    this.timestampsSeenBefore = new HashSet();
  }

  @Override
  protected void processMessage(DecodeableKafkaRecord message) {
    String specUri = (String) message.getKey();
    SpecStoreChangeEvent value = (SpecStoreChangeEvent) message.getValue();

    Long timestamp = value.getTimestamp();
    String operation = value.getOperationType().name();
    log.info("Processing message with specUri is {} timestamp is {} operation is {}", specUri, timestamp, operation);

    // If we've already processed a message with this timestamp before then skip duplicate message
    if (timestampsSeenBefore.contains(timestamp)) {
      return;
    }

    Spec spec = this.flowCatalog.getSpecFromStore(specUri);

    // Call respective action for the type of change received
    if (operation == "CREATE") {
      scheduler.onAddSpec(spec);
    } else if (operation == "INSERT") {
      scheduler.onUpdateSpec(spec);
    } else if (operation == "DELETE") {
      scheduler.onDeleteSpec(spec.getUri(), spec.getVersion());
    } else {
      log.warn("Received unsupported change type of operation {}. Expected values to be in [CREATE, INSERT, DELETE]", operation);
      return;
    }

    timestampsSeenBefore.add(timestamp);
  }
}