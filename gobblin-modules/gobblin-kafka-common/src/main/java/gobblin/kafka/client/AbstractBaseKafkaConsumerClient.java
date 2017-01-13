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
package gobblin.kafka.client;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import gobblin.source.extractor.extract.kafka.KafkaTopic;
import gobblin.util.DatasetFilterUtils;
import java.util.List;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ConfigUtils;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * A base {@link GobblinKafkaConsumerClient} that sets configurations shared by all {@link GobblinKafkaConsumerClient}s
 */
public abstract class AbstractBaseKafkaConsumerClient implements GobblinKafkaConsumerClient {

  public static final String CONFIG_PREFIX = "source.kafka.";
  public static final String CONFIG_KAFKA_FETCH_TIMEOUT_VALUE = CONFIG_PREFIX + "fetchTimeoutMillis";
  public static final int CONFIG_KAFKA_FETCH_TIMEOUT_VALUE_DEFAULT = 1000; // 1 second
  public static final String CONFIG_KAFKA_FETCH_REQUEST_MIN_BYTES = CONFIG_PREFIX + "fetchMinBytes";
  private static final int CONFIG_KAFKA_FETCH_REQUEST_MIN_BYTES_DEFAULT = 1024;
  public static final String CONFIG_KAFKA_SOCKET_TIMEOUT_VALUE = CONFIG_PREFIX + "socketTimeoutMillis";
  public static final int CONFIG_KAFKA_SOCKET_TIMEOUT_VALUE_DEFAULT = 30000; // 30 seconds

  protected final List<String> brokers;
  protected final int fetchTimeoutMillis;
  protected final int fetchMinBytes;
  protected final int socketTimeoutMillis;

  public AbstractBaseKafkaConsumerClient(Config config) {
    this.brokers = ConfigUtils.getStringList(config, ConfigurationKeys.KAFKA_BROKERS);
    if (this.brokers.isEmpty()) {
      throw new IllegalArgumentException("Need to specify at least one Kafka broker.");
    }

    this.socketTimeoutMillis =
        ConfigUtils.getInt(config, CONFIG_KAFKA_SOCKET_TIMEOUT_VALUE, CONFIG_KAFKA_SOCKET_TIMEOUT_VALUE_DEFAULT);
    this.fetchTimeoutMillis =
        ConfigUtils.getInt(config, CONFIG_KAFKA_FETCH_TIMEOUT_VALUE, CONFIG_KAFKA_FETCH_TIMEOUT_VALUE_DEFAULT);
    this.fetchMinBytes =
        ConfigUtils.getInt(config, CONFIG_KAFKA_FETCH_REQUEST_MIN_BYTES, CONFIG_KAFKA_FETCH_REQUEST_MIN_BYTES_DEFAULT);

    Preconditions.checkArgument((this.fetchTimeoutMillis < this.socketTimeoutMillis),
        "Kafka Source configuration error: FetchTimeout " + this.fetchTimeoutMillis
            + " must be smaller than SocketTimeout " + this.socketTimeoutMillis);
  }

  @Override
  public List<KafkaTopic> getFilteredTopics(final List<Pattern> blacklist, final List<Pattern> whitelist) {
    return Lists.newArrayList(Iterables.filter(getTopics(), new Predicate<KafkaTopic>() {
      @Override
      public boolean apply(@Nonnull KafkaTopic kafkaTopic) {
        return DatasetFilterUtils.survived(kafkaTopic.getName(), blacklist, whitelist);
      }
    }));
  }

  /**
   * Get a list of all kafka topics
   */
  public abstract List<KafkaTopic> getTopics();
}
