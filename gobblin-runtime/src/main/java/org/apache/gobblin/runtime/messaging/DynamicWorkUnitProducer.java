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
package org.apache.gobblin.runtime.messaging;

import java.io.IOException;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Producer runs in each task runner for sending {@link DynamicWorkUnitMessage} over a {@link MessageBuffer}
 * to be consumed by {@link DynamicWorkUnitConsumer} in the AM<br><br>
 *
 * A {@link DynamicWorkUnitProducer} has a tight coupling with the {@link DynamicWorkUnitConsumer}
 * since both producer / consumer should be using the same {@link MessageBuffer}
 * from the same {@link MessageBuffer.Factory} and using the same channel name
 */
public class DynamicWorkUnitProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicWorkUnitConsumer.class);
  private final MessageBuffer<DynamicWorkUnitMessage> messageBuffer;

  public DynamicWorkUnitProducer(MessageBuffer<DynamicWorkUnitMessage> messageBuffer) {
    this.messageBuffer = messageBuffer;
  }

  /**
   * Send a {@link DynamicWorkUnitMessage} to be consumed by a {@link DynamicWorkUnitConsumer}
   * @param message Message to be sent over the message buffer
   */
  public boolean produce(DynamicWorkUnitMessage message) throws IOException {
    LOG.debug("Sending message over message buffer, messageBuffer={}, message={}",
        messageBuffer.getClass().getSimpleName(), message);
    try {
      messageBuffer.publish(message);
      return true;
    } catch (IOException e) {
      LOG.debug("Failed to publish message. exception=", e);
      return false;
    }
  }
}
