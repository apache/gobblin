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

package org.apache.gobblin.service.monitoring;

import com.google.common.cache.LoadingCache;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ChangeMonitorUtils {
  private ChangeMonitorUtils() {
    return;
  }

  /**
   * Performs checks for duplicate messages and heartbeat operation prior to processing a message. Returns true if
   * the pre-conditions above don't apply and we should proceed processing the change event
   */
  public static boolean shouldProcessMessage(String changeIdentifier, LoadingCache<String, String> cache,
      String operation, String timestamp) {
    // If we've already processed a message with this timestamp and key before then skip duplicate message
    if (cache.getIfPresent(changeIdentifier) != null) {
      log.info("Duplicate change event with identifier {}", changeIdentifier);
      return false;
    }

    // If event is a heartbeat type then log it and skip processing
    if (operation.equals("HEARTBEAT")) {
      log.debug("Received heartbeat message from time {}", timestamp);
      return false;
    }

    return true;
  }
}
