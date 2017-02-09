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

package gobblin.kafka.writer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import com.google.common.base.Throwables;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.writer.AsyncDataWriter;
import gobblin.writer.StreamCodec;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;
import gobblin.writer.WriteResponseFuture;
import gobblin.writer.WriteResponseMapper;


/**
 * Implementation of a Kafka writer that wraps a 0.8 {@link KafkaProducer}.
 * This does not provide transactional / exactly-once semantics.
 * Applications should expect data to be possibly written to Kafka even if the overall Gobblin job fails.
 *
 */
@Slf4j
public class Kafka08DataWriter<D> implements AsyncDataWriter<D> {

  private static final WriteResponseMapper<RecordMetadata> WRITE_RESPONSE_WRAPPER =
      new WriteResponseMapper<RecordMetadata>() {

        @Override
        public WriteResponse wrap(final RecordMetadata recordMetadata) {
          return new WriteResponse<RecordMetadata>() {
            @Override
            public RecordMetadata getRawResponse() {
              return recordMetadata;
            }

            @Override
            public String getStringResponse() {
              return recordMetadata.toString();
            }

            @Override
            public long bytesWritten() {
              // Don't know how many bytes were written
              return -1;
            }
          };
        }
      };

  private final Producer<String, byte[]> producer;
  private final String topic;
  private final List<StreamCodec> encoders;
  private final Serializer<D> valueSerializer;

  public Producer<String, byte[]> getKafkaProducer(Properties props)
  {
    Object producerObject = KafkaWriterHelper.getKafkaProducer(props);
    try
    {
      @SuppressWarnings("unchecked")
      Producer<String, byte[]> producer = (Producer<String, byte[]>) producerObject;

      return producer;
    } catch (ClassCastException e) {
      log.error("Failed to instantiate Kafka producer " + producerObject.getClass().getName() + " as instance of Producer.class", e);
      throw Throwables.propagate(e);
    }
  }


  @SuppressWarnings("unchecked")
  public Kafka08DataWriter(Properties props, List<StreamCodec> encoders) {
    this.producer = getKafkaProducer(props);
    this.valueSerializer = (Serializer<D>)KafkaWriterHelper.getValueSerializer(Serializer.class, props);
    this.encoders = encoders;
    this.topic = ConfigFactory.parseProperties(props).getString(KafkaWriterConfigurationKeys.KAFKA_TOPIC);
  }

  @Override
  public void close()
      throws IOException {
    log.debug("Close called");
    this.producer.close();
  }



  @Override
  public Future<WriteResponse> write(final D record, final WriteCallback callback) {
    try {
      byte[] recordBytes = serializeRecord(record);
      return new WriteResponseFuture<>(this.producer.send(new ProducerRecord<String, byte[]>(topic, recordBytes), new Callback() {
        @Override
        public void onCompletion(final RecordMetadata metadata, Exception exception) {
          if (exception != null) {
            callback.onFailure(exception);
          } else {
            callback.onSuccess(WRITE_RESPONSE_WRAPPER.wrap(metadata));
          }
        }
      }), WRITE_RESPONSE_WRAPPER);
    } catch (IOException e) {
      log.warn("Error encoding record", e);
      if (callback != null) {
        callback.onFailure(e);
      }

      return new FailureFuture<WriteResponse>(e);
    }
  }

  private byte[] serializeRecord(D record) throws IOException {
    byte[] recordBytes = this.valueSerializer.serialize(this.topic, record);

    if (this.encoders.size() > 0) {
      ByteArrayOutputStream byteOs = new ByteArrayOutputStream();
      OutputStream encodedStream = byteOs;
      for (StreamCodec e: encoders) {
        encodedStream = e.wrapOutputStream(encodedStream);
      }

      encodedStream.write(recordBytes);
      encodedStream.close();
      recordBytes = byteOs.toByteArray();
    }

    return recordBytes;
  }

  @Override
  public void flush()
      throws IOException {
    // Do nothing, 0.8 kafka producer doesn't support flush.
  }
}
