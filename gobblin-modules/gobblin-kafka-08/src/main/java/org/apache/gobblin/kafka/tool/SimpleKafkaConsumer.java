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

package org.apache.gobblin.kafka.tool;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistry;
import org.apache.gobblin.kafka.schemareg.KafkaSchemaRegistryFactory;
import org.apache.gobblin.kafka.serialize.LiAvroDeserializer;
import org.apache.gobblin.kafka.serialize.MD5Digest;


/**
 * A simple kafka consumer for debugging purposes.
 */
@Slf4j
public class SimpleKafkaConsumer {


  private final ConsumerConnector consumer;
  private final KafkaStream<byte[], byte[]> stream;
  private final ConsumerIterator<byte[], byte[]> iterator;
  private final String topic;
  private final KafkaSchemaRegistry<MD5Digest, Schema> schemaRegistry;
  private final Deserializer deserializer;

  public SimpleKafkaConsumer(Properties props, KafkaCheckpoint checkpoint)
  {
    Config config = ConfigFactory.parseProperties(props);
    topic = config.getString("topic");
    String zkConnect = config.getString("zookeeper.connect");

    schemaRegistry = KafkaSchemaRegistryFactory.getSchemaRegistry(props);
    deserializer = new LiAvroDeserializer(schemaRegistry);
    /** TODO: Make Confluent schema registry integration configurable
     * HashMap<String, String> avroSerDeConfig = new HashMap<>();
     * avroSerDeConfig.put("schema.registry.url", "http://localhost:8081");
     * deserializer = new io.confluent.kafka.serializers.KafkaAvroDeserializer();
     * deserializer.configure(avroSerDeConfig, false);
     *
     **/

    Properties consumeProps = new Properties();
    consumeProps.put("zookeeper.connect", zkConnect);
    consumeProps.put("group.id", "gobblin-tool-" + System.nanoTime());
    consumeProps.put("zookeeper.session.timeout.ms", "10000");
    consumeProps.put("zookeeper.sync.time.ms", "10000");
    consumeProps.put("auto.commit.interval.ms", "10000");
    consumeProps.put("auto.offset.reset", "smallest");
    consumeProps.put("auto.commit.enable", "false");
    //consumeProps.put("consumer.timeout.ms", "10000");

    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumeProps));

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(ImmutableMap.of(topic, 1));
    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(this.topic);
    stream = streams.get(0);

    iterator = stream.iterator();
  }


  public void close()
  {
    consumer.shutdown();
  }




  public void shutdown()
  {
    close();
  }
  public static void main(String[] args)
      throws IOException {
    Preconditions.checkArgument(args.length>=1, "Usage: java " + SimpleKafkaConsumer.class.getName() + " <properties_file> <checkpoint_file>");
    String fileName = args[0];
    Properties props = new Properties();
    props.load(new FileInputStream(new File(fileName)));

    KafkaCheckpoint checkpoint = KafkaCheckpoint.emptyCheckpoint();
    File checkpointFile = null;
    if (args.length > 1)
    {
      try {
        checkpointFile = new File(args[1]);
        if (checkpointFile.exists()) {
          FileInputStream fis = null;
          try {
            fis = new FileInputStream(checkpointFile);
            checkpoint = KafkaCheckpoint.deserialize(fis);
          } finally {
            if (fis != null) fis.close();
          }
        } else {
          log.info("Checkpoint doesn't exist, we will start with an empty one and store it here.");
        }
      }
      catch (IOException e)
      {
        log.warn("Could not deserialize the previous checkpoint. Starting with empty", e);
        if (!checkpoint.isEmpty())
        {
          checkpoint = KafkaCheckpoint.emptyCheckpoint();
        }
      }
    }

    final SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(props, checkpoint);
    Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run()
        {
          log.info("Shutting down...");
          consumer.shutdown();
        }
    });
    consumer.printLoop(checkpoint, checkpointFile);
  }

  private void printLoop(KafkaCheckpoint checkpoint, File checkpointFile)
      throws IOException {

    boolean storeCheckpoints = (checkpointFile != null);
    if (storeCheckpoints)
    {
      boolean newFileCreated = checkpointFile.createNewFile();
      if (newFileCreated) {
        log.info("Created new checkpoint file: " + checkpointFile.getAbsolutePath());
      }
    }
    while (true)
    {
      MessageAndMetadata<byte[], byte[]> messagePlusMeta;
      try {
        if (!iterator.hasNext()) {
          return;
        }
        messagePlusMeta = iterator.next();
        if (messagePlusMeta!=null) {
          byte[] payload = messagePlusMeta.message();
          System.out.println("Got a message of size " + payload.length + " bytes");
          GenericRecord record = (GenericRecord) deserializer.deserialize(topic, payload);
          System.out.println(record.toString());
          checkpoint.update(messagePlusMeta.partition(), messagePlusMeta.offset());
        }
      }
      catch (RuntimeException e)
      {
        log.warn("Error detected", e);
      }
      finally
      {
        if (storeCheckpoints) {
          if (checkpoint != KafkaCheckpoint.emptyCheckpoint()) {
            System.out.println("Storing checkpoint to file: " + checkpointFile.getAbsolutePath());
            KafkaCheckpoint.serialize(checkpoint, checkpointFile);
          }
        }
      }
    }
  }
}
