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

package org.apache.gobblin.metrics.kafka;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.reporter.EventReporter;
import org.apache.gobblin.util.AvroUtils;

@Slf4j
public class KafkaKeyValueEventObjectReporter extends EventReporter {

  protected Optional<List<String>> keys = Optional.absent();
  protected final String randomKey;
  protected KeyValuePusher kafkaPusher;
  protected Optional<Map<String,String>> namespaceOverride;
  protected final String topic;

  public KafkaKeyValueEventObjectReporter(Builder<?> builder){
    super(builder);

    this.topic=builder.topic;
    this.namespaceOverride=builder.namespaceOverride;

    if (builder.kafkaPusher.isPresent()) {
      this.kafkaPusher = builder.kafkaPusher.get();
    } else {
      this.kafkaPusher = PusherUtils.getKeyValuePusher(builder.pusherClassName.get(), builder.brokers, builder.topic, builder.config);
    }

    this.closer.register(this.kafkaPusher);

    randomKey = String.valueOf(new Random().nextInt(100));
    if (builder.keys.size() > 0) {
      this.keys = Optional.of(builder.keys);
    } else {
      log.warn("Key not assigned from config. Please set it with property {}", ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS);
      log.warn("Using generated number " + randomKey + " as key");
    }
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    log.info("Emitting report using KafkaKeyValueEventObjectReporter");

    List<Pair<String,GenericRecord>> events = Lists.newArrayList();
    GobblinTrackingEvent event;

    while(null != (event = queue.poll())){
      GenericRecord record = AvroUtils.overrideNameAndNamespace(event, this.topic, this.namespaceOverride);
      events.add(Pair.of(buildKey(event), record));
    }

    if (!events.isEmpty()) {
      this.kafkaPusher.pushKeyValueMessages(events);
    }
  }

  protected String buildKey(GobblinTrackingEvent event) {

    String key = randomKey;
    if (this.keys.isPresent()) {

      StringBuilder keyBuilder = new StringBuilder();
      for (String keyPart : keys.get()) {
        Map<String,String> metadata = event.getMetadata();
        if (metadata.containsKey(keyPart)) {
          keyBuilder.append(metadata.get(keyPart));
        } else {
          log.error("{} not found in the GobblinTrackingEvent. Setting key to null.", key);
          keyBuilder = null;
          break;
        }
      }

      key = (keyBuilder == null) ? key : keyBuilder.toString();
    }

    return key;
  }

  public static class Factory {
    /**
     * Returns a new {@link KafkaKeyValueEventObjectReporter.Builder} for {@link KafkaKeyValueEventObjectReporter}.
     * Will automatically add all Context tags to the reporter.
     *
     * @param context the {@link MetricContext} to report
     * @return KafkaReporter builder
     */
    public static BuilderImpl forContext(MetricContext context) {
      return new BuilderImpl(context);
    }
  }

  public static class BuilderImpl extends Builder<BuilderImpl> {
    private BuilderImpl(MetricContext context) {
      super(context);
    }

    @Override
    protected BuilderImpl self() {
      return this;
    }
  }

  public static abstract class Builder<T extends Builder<T>> extends EventReporter.Builder<T>{

    protected List<String> keys = Lists.newArrayList();
    protected String brokers;
    protected String topic;
    protected Optional<KeyValuePusher> kafkaPusher=Optional.absent();
    protected Optional<Config> config = Optional.absent();
    protected Optional<String> pusherClassName = Optional.absent();
    protected Optional<Map<String,String>> namespaceOverride=Optional.absent();

    protected Builder(MetricContext context) {
      super(context);
    }
    /**
     * Set {@link Pusher} to use.
     */
    public T withKafkaPusher(KeyValuePusher pusher) {
      this.kafkaPusher = Optional.of(pusher);
      return self();
    }

    /**
     * Set additional configuration.
     */
    public T withConfig(Config config) {
      this.config = Optional.of(config);
      return self();
    }

    /**
     * Set a {@link Pusher} class name
     */
    public T withPusherClassName(String pusherClassName) {
      this.pusherClassName = Optional.of(pusherClassName);
      return self();
    }

    public T withKeys(List<String> keys) {
      this.keys = keys;
      return self();
    }

    public T namespaceOverride(Optional<Map<String,String>> namespaceOverride) {
      this.namespaceOverride = namespaceOverride;
      return self();
    }

    public KafkaKeyValueEventObjectReporter build(String brokers, String topic){
      this.brokers=brokers;
      this.topic=topic;
      return new KafkaKeyValueEventObjectReporter(this);
    }

  }

//  public static GenericRecord overrideNameAndNamespace(GobblinTrackingEvent event, String nameOverride, Optional<Map<String, String>> namespaceOverride) {
//
//    GenericRecord record = event;
//    Schema newSchema = AvroUtils.switchName(event.getSchema(), nameOverride);
//    if(namespaceOverride.isPresent()) {
//      newSchema = AvroUtils.switchNamespace(newSchema, namespaceOverride.get());
//    }
//
//    try {
//      record = AvroUtils.convertRecordSchema(record, newSchema);
//    } catch (Exception e){
//      log.error("Unable to generate generic data record", e);
//    }
//
//    return record;
//  }

}
