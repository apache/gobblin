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
package org.apache.gobblin.runtime.messaging.data;

import gobblin.source.workunit.WorkUnit;

/**
 * Generic message for sending updates about a workunit during runtime. Implementations can
 * extend this interface with other getters / properties to piggyback information specific to the message subtype.
 *
 * For example, the {@link SplitWorkUnitMessage} extends this interface by adding fields that are not specified in this
 * interface.
 */
public interface DynamicWorkUnitMessage {
  /**
   * The WorkUnit Id this message is associated with. Same as {@link WorkUnit#getId()}
   * @return WorkUnit Id
   */
  String getWorkUnitId();

  /**
   * Handler for processing messages and implementing business logic
   */
  interface Handler {
    /**
     * Process this message by handling business logic
     * @param message
     */
    void handle(DynamicWorkUnitMessage message);
  }
}
