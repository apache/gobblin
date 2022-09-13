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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.gobblin.runtime.messaging.data.DynamicWorkUnitMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Receives {@link DynamicWorkUnitMessage} sent by {@link DynamicWorkUnitProducer}.
 * The class is used as a callback for the {@link MessageBuffer}. All business logic
 * is done in the {@link DynamicWorkUnitMessage.Handler}.<br><br>
 *
 * {@link DynamicWorkUnitConsumer#accept(List)} is the entrypoint for processing {@link DynamicWorkUnitMessage}
 * received by the {@link MessageBuffer} after calling {@link MessageBuffer#subscribe(Consumer)}<br><br>
 *
 * Each newly published {@link DynamicWorkUnitMessage} is passed to a {@link DynamicWorkUnitMessage.Handler}
 * and will call {@link DynamicWorkUnitMessage.Handler#handle(DynamicWorkUnitMessage)} to do business logic
 */
public class DynamicWorkUnitConsumer implements Consumer<List<DynamicWorkUnitMessage>> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicWorkUnitConsumer.class);
  private final List<DynamicWorkUnitMessage.Handler> messageHandlers;

  public DynamicWorkUnitConsumer(Collection<DynamicWorkUnitMessage.Handler> handlers) {
    this.messageHandlers = new ArrayList<>(handlers);
  }

  /**
   * Entry point for processing messages sent by {@link DynamicWorkUnitProducer} via {@link MessageBuffer}. This
   * calls {@link DynamicWorkUnitMessage.Handler#handle(DynamicWorkUnitMessage)} method for each handler added via
   * {@link DynamicWorkUnitConsumer#DynamicWorkUnitConsumer(Collection<DynamicWorkUnitMessage.Handler>)
   */
  @Override
  public void accept(List<DynamicWorkUnitMessage> messages) {
    for (DynamicWorkUnitMessage msg : messages) {
      handleMessage(msg);
    }
  }

  private void handleMessage(DynamicWorkUnitMessage msg) {
    LOG.debug("{} handling message={}", DynamicWorkUnitConsumer.class.getSimpleName(), msg);
    for (DynamicWorkUnitMessage.Handler handler : this.messageHandlers) {
      handler.handle(msg);
    }
  }
}
