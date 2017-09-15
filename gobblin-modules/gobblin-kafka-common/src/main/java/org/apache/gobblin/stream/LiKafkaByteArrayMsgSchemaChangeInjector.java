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
package org.apache.gobblin.stream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistry;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryConfigurationKeys;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryFactory;
import org.apache.gobblin.kafka.schemareg.SchemaRegistryException;
import org.apache.gobblin.kafka.serialize.LiAvroSerDeHelper;
import org.apache.gobblin.kafka.serialize.MD5Digest;
import org.apache.gobblin.metadata.GlobalMetadata;
import org.apache.gobblin.source.extractor.extract.kafka.KafkaSource;

import lombok.AccessLevel;
import lombok.Getter;

/**
 * A {@link ControlMessageInjector} that detects changes in the latest schema and notifies downstream constructs by
 * injecting a {@link MetadataUpdateControlMessage}.
 * @param <S>
 */
public class LiKafkaByteArrayMsgSchemaChangeInjector<S> extends ControlMessageInjector<S, byte[]> {
  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private KafkaSchemaRegistry schemaRegistry;

  @VisibleForTesting
  @Getter(AccessLevel.PACKAGE)
  private Cache<ByteBuffer, String> schemaIdCache;
  private GlobalMetadata<S> globalMetadata;
  private String topicName;

  @Override
  public ControlMessageInjector<S, byte[]> init(WorkUnitState workUnitState) {
    Preconditions.checkArgument(workUnitState.contains(KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS),
        "Missing required property " + KafkaSchemaRegistryConfigurationKeys.KAFKA_SCHEMA_REGISTRY_CLASS);

    this.schemaRegistry = KafkaSchemaRegistryFactory.getSchemaRegistry(workUnitState.getProperties());
    this.schemaIdCache = CacheBuilder.newBuilder()
        .expireAfterAccess(1, TimeUnit.HOURS)
        .build();
    this.topicName = workUnitState.getProp(KafkaSource.TOPIC_NAME);

    return this;
  }

  @Override
  public void setInputGlobalMetadata(GlobalMetadata<S> inputGlobalMetadata, WorkUnitState workUnitState) {
    this.globalMetadata = inputGlobalMetadata;
  }

  /**
   * Inject a {@link MetadataUpdateControlMessage} if the latest schema has changed.
   * @param inputRecordEnvelope input record envelope
   * @param workUnitState work unit state
   * @return the injected messages
   */
  @Override
  public Iterable<ControlMessage<byte[]>> injectControlMessagesBefore(RecordEnvelope<byte[]> inputRecordEnvelope,
                                                                      WorkUnitState workUnitState) {
    byte[] data = inputRecordEnvelope.getRecord();

    // magic byte should be there for messages from LiKafka
    if (inputRecordEnvelope.getRecord()[0] != LiAvroSerDeHelper.MAGIC_BYTE) {
      throw new RuntimeException(String.format("Unknown magic byte for topic: %s ", topicName));
    }

    // New schema id so check if the latest schema in the registry has changed.
    // Only check for the latest schema when a new schema id is seen since the call to get the latest schema is not
    // cacheable and is expensive.
    if (this.schemaIdCache.getIfPresent(ByteBuffer.wrap(data, 1, MD5Digest.MD5_BYTES_LENGTH)) == null) {
      try {
        S latestSchema = (S) this.schemaRegistry.getLatestSchema(topicName);

        // copy to a new byte buffer to avoid holding a reference to the whole record
        this.schemaIdCache.put(ByteBuffer.wrap(Arrays.copyOfRange(data, 1, MD5Digest.MD5_BYTES_LENGTH + 1)), "");

        // latest schema changed, so inject a metadata update control message
        if (!latestSchema.equals(this.globalMetadata.getSchema())) {
          // update the metadata in this injector since the control message is only applied downstream
          this.globalMetadata = GlobalMetadata.<S, S>builderWithInput(this.globalMetadata,
              Optional.of(latestSchema)).build();

          // inject a metadata update control message followed by a flush control message since the avro data needs to
          // be flushed and a new file opened to write data in the new schema
          return Lists.newArrayList(new MetadataUpdateControlMessage<S, byte[]>(this.globalMetadata),
              FlushControlMessage.<byte[]>builder().flushReason("Metadata change")
                  .flushType(FlushControlMessage.FlushType.FLUSH_AND_CLOSE).build());
        }
      } catch (IOException | SchemaRegistryException e) {
        throw new RuntimeException("Exception when getting the latest schema for topic " + topicName, e);
      }
    }

    // no schema change detected
    return null;
  }

  @Override
  public Iterable<ControlMessage<byte[]>> injectControlMessagesAfter(RecordEnvelope<byte[]> inputRecordEnvelope,
                                                                     WorkUnitState workUnitState) {
    return null;
  }
}
