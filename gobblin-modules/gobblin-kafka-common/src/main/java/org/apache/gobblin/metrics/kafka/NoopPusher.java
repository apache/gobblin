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

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;


/**
 * This is a {@Pusher} class that ignores the messages
 * @param <M> message type
 */
@Slf4j
public class NoopPusher<M> implements Pusher<M> {
  public NoopPusher() {}

  public NoopPusher(Config config) {}

  /**
   * Constructor like the one in KafkaProducerPusher for compatibility
   */
  public NoopPusher(String brokers, String topic, Optional<Config> kafkaConfig) {}

  public void pushMessages(List<M> messages) {}

  @Override
  public void close() throws IOException {}
}
