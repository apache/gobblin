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
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.reporter.EventReporter;
import org.apache.gobblin.util.AvroUtils;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
public class KeyValueEventObjectReporter extends EventReporter {
  private static final String PUSHER_CONFIG = "pusherConfig";
  private static final String PUSHER_CLASS = "pusherClass";
  private static final String PUSHER_KEYS = "pusherKeys";

  protected Optional<List<String>> keys = Optional.absent();
  protected final String randomKey;
  protected KeyValuePusher pusher;
  protected Optional<Map<String,String>> namespaceOverride;
  protected final String topic;

  public KeyValueEventObjectReporter(Builder<?> builder){
    super(builder);

    this.topic=builder.topic;
    this.namespaceOverride=builder.namespaceOverride;

    Config config = builder.config.get();
    Config pusherConfig = ConfigUtils.getConfigOrEmpty(config, PUSHER_CONFIG).withFallback(config);
    String pusherClassName = ConfigUtils.getString(config, PUSHER_CLASS, PusherUtils.DEFAULT_KEY_VALUE_PUSHER_CLASS_NAME);
    this.pusher = PusherUtils.getKeyValuePusher(pusherClassName, builder.brokers, builder.topic, Optional.of(pusherConfig));
    this.closer.register(this.pusher);

    randomKey=String.valueOf(new Random().nextInt(100));
    if (config.hasPath(PUSHER_KEYS)) {
      List<String> keys = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(config.getString(PUSHER_KEYS));
      this.keys = Optional.of(keys);
    }else{
      log.warn("Key not assigned from config. Please set it with property {}", ConfigurationKeys.METRICS_REPORTING_EVENTS_KAFKAPUSHERKEYS);
      log.warn("Using generated number " + randomKey + " as key");
    }
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    log.info("Emitting report using KeyValueEventObjectReporter");

    List<Pair<String,GenericRecord>> events = Lists.newArrayList();
    GobblinTrackingEvent event;

    while(null != (event = queue.poll())){
      GenericRecord record = AvroUtils.overrideNameAndNamespace(event, this.topic, this.namespaceOverride);
      events.add(Pair.of(buildKey(event), record));
    }

    if (!events.isEmpty()) {
      this.pusher.pushKeyValueMessages(events);
    }
  }

  private String buildKey(GobblinTrackingEvent event) {

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
     * Returns a new {@link KeyValueEventObjectReporter.Builder} for {@link KeyValueEventObjectReporter}.
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

    protected String brokers;
    protected String topic;
    protected Optional<Config> config = Optional.absent();
    protected Optional<Map<String,String>> namespaceOverride=Optional.absent();

    protected Builder(MetricContext context) {
      super(context);
    }

    /**
     * Set additional configuration.
     */
    public T withConfig(Config config) {
      this.config = Optional.of(config);
      return self();
    }

    public T namespaceOverride(Optional<Map<String,String>> namespaceOverride) {
      this.namespaceOverride = namespaceOverride;
      return self();
    }

    public KeyValueEventObjectReporter build(String brokers, String topic){
      this.brokers=brokers;
      this.topic=topic;
      return new KeyValueEventObjectReporter(this);
    }
  }
}
