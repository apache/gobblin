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

import java.io.Closeable;
import java.util.List;


/**
 * Establish a connection to a Kafka cluster and push byte messages to a specified topic.
 */
public interface Pusher extends Closeable {
  /**
   * Push all byte array messages to the Kafka topic.
   * @param messages List of byte array messages to push to Kakfa.
   */
  void pushMessages(List<byte[]> messages);
}
