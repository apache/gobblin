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

package org.apache.gobblin.converter;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import lombok.AccessLevel;
import lombok.Getter;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.client.DecodeableKafkaRecord;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.metrics.kafka.KafkaSchemaRegistry;
import org.apache.gobblin.metrics.kafka.SchemaRegistryException;
import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.ControlMessageInjector;
import org.apache.gobblin.stream.MetadataUpdateControlMessage;
import org.apache.gobblin.stream.RecordEnvelope;


/**
 * A {@link org.apache.gobblin.stream.MetadataUpdateControlMessage} that detects changes in the latest schema and notifies downstream constructs by
 * injecting a {@link org.apache.gobblin.stream.MetadataUpdateControlMessage}.
 */
public abstract class KafkaSchemaChangeInjector<S>
    extends ControlMessageInjector<Schema, DecodeableKafkaRecord> {
  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private KafkaSchemaRegistry<String, Schema> schemaRegistry;

  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private Cache<S, String> schemaCache;
  private Schema latestSchema;
  private GlobalMetadata<Schema> globalMetadata;

  // classes that extend this need to implement getSchemaIdentifier
  protected abstract S getSchemaIdentifier(DecodeableKafkaRecord consumerRecord);

  @Override
  public ControlMessageInjector<Schema, DecodeableKafkaRecord> init(WorkUnitState workUnitState) {
    this.schemaRegistry = KafkaSchemaRegistry.get(workUnitState.getProperties());
    this.schemaCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();
    return this;
  }

  @Override
  public void setInputGlobalMetadata(GlobalMetadata<Schema> inputGlobalMetadata, WorkUnitState workUnitState) {
    this.globalMetadata = inputGlobalMetadata;
  }

  /**
   * Inject a {@link org.apache.gobblin.stream.MetadataUpdateControlMessage} if the latest schema has changed. Check whether there is a new latest
   * schema if the input record's schema is not present in the schema cache.
   *
   * @param inputRecordEnvelope input record envelope
   * @param workUnitState work unit state
   * @return the injected messages
   */
  @Override
  public Iterable<ControlMessage<DecodeableKafkaRecord>> injectControlMessagesBefore(
      RecordEnvelope<DecodeableKafkaRecord> inputRecordEnvelope, WorkUnitState workUnitState) {
    DecodeableKafkaRecord consumerRecord = inputRecordEnvelope.getRecord();
    S schemaIdentifier = getSchemaIdentifier(consumerRecord);
    String topicName = consumerRecord.getTopic();

    // If a new schema is seen then check the latest schema in the registry has changed.
    // Only check for the latest schema when a new schema is seen since the call to get the latest schema is not
    // cacheable and is expensive.
    if (this.schemaCache.getIfPresent(schemaIdentifier) == null) {
      try {
        Schema latestSchema = this.schemaRegistry.getLatestSchemaByTopic(topicName);

        this.schemaCache.put(schemaIdentifier, "");

        // latest schema changed, so inject a metadata update control message
        if (!latestSchema.equals(this.latestSchema)) {
          // update the metadata in this injector since the control message is only applied downstream
          this.globalMetadata = GlobalMetadata.builderWithInput(this.globalMetadata, Optional.of(latestSchema)).build();
          // update the latestSchema
          this.latestSchema = latestSchema;
          // inject a metadata update control message before the record so that the downstream constructs
          // are aware of the new schema before processing the record
          ControlMessage datasetLevelMetadataUpdate = new MetadataUpdateControlMessage(this.globalMetadata);

          return Collections.singleton(datasetLevelMetadataUpdate);
        }
      } catch (SchemaRegistryException e) {
        throw new RuntimeException("Exception when getting the latest schema for topic " + topicName, e);
      }
    }

    // no schema change detected
    return null;
  }

  @Override
  public Iterable<ControlMessage<DecodeableKafkaRecord>> injectControlMessagesAfter(
      RecordEnvelope<DecodeableKafkaRecord> inputRecordEnvelope, WorkUnitState workUnitState) {
    return null;
  }
}