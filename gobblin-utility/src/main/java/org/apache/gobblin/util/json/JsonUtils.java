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
package org.apache.gobblin.util.json;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * JSON related utilities
 */
public class JsonUtils {
  private static final JsonFactory jacksonFactory = new JsonFactory();
  private static final ObjectMapper jacksonObjectMapper = new ObjectMapper();

  /**
   * Get a Jackson JsonFactory configured with standard settings. The JsonFactory is thread-safe.
   */
  public static JsonFactory getDefaultJacksonFactory() {
    return jacksonFactory;
  }

  /**
   * Get a Jackson ObjectMapper configured with standard settings. The ObjectMapper is thread-safe.
   */
  public static ObjectMapper getDefaultObjectMapper() {
    return jacksonObjectMapper;
  }
}
