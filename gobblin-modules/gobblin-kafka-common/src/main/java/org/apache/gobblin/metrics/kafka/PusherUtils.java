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

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import org.apache.gobblin.util.reflection.GobblinConstructorUtils;

public class PusherUtils {
  public static final String METRICS_REPORTING_KAFKA_CONFIG_PREFIX = "metrics.reporting.kafka.config";
  public static final String KAFKA_PUSHER_CLASS_NAME_KEY = "metrics.reporting.kafkaPusherClass";
  public static final String DEFAULT_KAFKA_PUSHER_CLASS_NAME = "org.apache.gobblin.metrics.kafka.KafkaPusher";

  /**
   * Create a {@link Pusher}
   * @param pusherClassName the {@link Pusher} class to instantiate
   * @param brokers brokers to connect to
   * @param topic the topic to write to
   * @param config additional configuration for configuring the {@link Pusher}
   * @return a {@link Pusher}
   */
  public static Pusher getPusher(String pusherClassName, String brokers, String topic, Optional<Config> config) {
    try {
      Class<?> pusherClass = Class.forName(pusherClassName);

     return (Pusher) GobblinConstructorUtils.invokeLongestConstructor(pusherClass,
          brokers, topic, config);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not instantiate kafka pusher", e);
    }
  }
}
