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

package org.apache.gobblin.multistage.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


/**
 * Define a structure for data interchange between a Source and a Extractor.
 *
 * @author chrli
 */
@Slf4j
@Data
@Builder(toBuilder = true)
public class WorkUnitStatus {
  private long totalCount;
  private long setCount;
  private long pageNumber = 0;
  private long pageStart = 0;
  private long pageSize = 0;
  private InputStream buffer;

  private Map<String, String> messages;
  private String sessionKey = "";

  /**
   *  retrieve source schema if provided
   *
   * @return source schema if provided
   */
  public JsonSchema getSchema() {
    if (messages != null && messages.containsKey("schema")) {
      try {
        return new JsonSchema(new Gson().fromJson(messages.get("schema"), JsonObject.class));
      } catch (Exception e) {
        log.warn("Error reading source schema", e);
      }
    }
    return new JsonSchema();
  }

  public void logDebugAll() {
    log.debug("These are values in WorkUnitStatus");
    log.debug("Total count: {}", totalCount);
    log.debug("Chunk count: {}", setCount);
    log.debug("Pagination: {},{},{}", pageStart, pageSize, pageNumber);
    log.debug("Session Status: {}", sessionKey);
  }
}
