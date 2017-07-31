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
package org.apache.gobblin.records;

import java.io.Flushable;
import java.io.IOException;

import org.apache.gobblin.stream.ControlMessage;
import org.apache.gobblin.stream.FlushControlMessage;


/**
 * Flush control message handler that will flush a {@link Flushable} when handling a {@link FlushControlMessage}
 */
public class FlushControlMessageHandler implements ControlMessageHandler {
  Flushable flushable;

  /**
   * Create a flush control message that will flush the given {@link Flushable}
   * @param flushable the flushable to flush when a {@link FlushControlMessage} is received
   */
  public FlushControlMessageHandler(Flushable flushable) {
    this.flushable = flushable;
  }

  @Override
  public void handleMessage(ControlMessage message) {
    if (message instanceof FlushControlMessage) {
      try {
        flushable.flush();
      } catch (IOException e) {
        throw new RuntimeException("Could not flush when handling FlushControlMessage", e);
      }
    }
  }
}
