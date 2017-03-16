/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package gobblin.type;

import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;


/**
 * Utilities to work with MIME content-types
 */
@Slf4j
public class ContentTypeUtils {
  private static final ContentTypeUtils INSTANCE = new ContentTypeUtils();

  public static ContentTypeUtils getInstance() {
    return INSTANCE;
  }

  private ConcurrentHashMap<String, String> knownCharsets;

  /**
   * Check which character set a given content-type corresponds to.
   * @param contentType Content-type to check
   * @return Charset the mimetype represents. "BINARY" if binary data.
   */
  public String getCharset(String contentType) {
    String charSet = knownCharsets.get(contentType);
    if (charSet != null) {
      return charSet;
    }

    // Special cases
    if (contentType.startsWith("text/") || contentType.endsWith("+json") || contentType.endsWith("+xml")) {
      return "UTF-8";
    }

    return "BINARY";
  }

  /**
   * Register a new contentType to charSet mapping.
   * @param contentType Content-type to register
   * @param charSet charSet associated with the content-type
   */
  public void registerCharsetMapping(String contentType, String charSet) {
    if (knownCharsets.contains(contentType)) {
      log.warn("{} is already registered; re-registering");
    }

    knownCharsets.put(contentType, charSet);
  }

  private ContentTypeUtils() {
    knownCharsets = new ConcurrentHashMap<>();
    knownCharsets.put("base64", "UTF-8");
    knownCharsets.put("application/xml", "UTF-8");
    knownCharsets.put("application/json", "UTF-8");
  }
}
