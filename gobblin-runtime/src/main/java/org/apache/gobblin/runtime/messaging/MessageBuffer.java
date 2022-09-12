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
import java.util.List;
import java.util.function.Consumer;


/**
 * Unidirectional buffer for sending messages from container to container.
 * Example use case is for sending messages from taskrunner -> AM. If messages need to be
 * sent bi-directionally, use multiple message buffers with different channel names.<br><br>
 *
 * {@link MessageBuffer} should only be instantiated using {@link MessageBuffer.Factory#getBuffer(String)} and not via constructor.
 * A {@link MessageBuffer} will only communicate with other {@link MessageBuffer}(s) created by the same {@link MessageBuffer.Factory}
 * and the same channel name.
 *
 * This interface provides the following guarantees:
 * <ul>
 *   <li>No guaranteed order delivery</li>
 *   <li>Single reader calling subscribe method</li>
 *   <li>Multiple concurrent writers calling publish</li>
 *   <li>Messages delivered at most once</li>
 * </ul>
 */
public interface MessageBuffer<T> {
  /**
   * Alias for the message buffer. Message buffers will only communicate with other message buffers
   * using the same channel name and coming from the same {@link MessageBuffer.Factory}
   *
   * i.e. When 2 containers use the same factory implementation to create a {@link MessageBuffer} with the same
   * {@link MessageBuffer#getChannelName()}, they should be able to send messages uni-directionally from
   * publisher to subscriber.
   * @return channel name
   */
  String getChannelName();

  /**
   * Publish item to message buffer for consumption by subscribers
   * @param item item to publish to subscribers
   * @return Is item successfully added to buffer
   * @throws IOException if unable to add message to buffer
   */
  void publish(T item) throws IOException;

  /**
   * Add callback {@link Consumer} object that will consume all published {@link T}. It is safe to subscribe multiple
   * consumer objects but only one container should be calling this method per channel. This is
   * because the message buffer API supports at-most once delivery and one reader, so reads are destructive
   */
  void subscribe(Consumer<List<T>> consumer);

  /**
   * Factory for instantiating {@link MessageBuffer} with a specific channel name. Message buffers produced by the same
   * factory and using the same channel name are able to communicate with each other.
   * @param <T>
   */
  interface Factory<T> {
    /**
     * Create {@link MessageBuffer} with specific channel name. {@link MessageBuffer}(s) with the same channel name and
     * from the same factory will exclusively communicate with eachother.
     * @param channelName channel namespace
     * @return Message Buffer
     */
    MessageBuffer<T> getBuffer(String channelName);
  }
}
