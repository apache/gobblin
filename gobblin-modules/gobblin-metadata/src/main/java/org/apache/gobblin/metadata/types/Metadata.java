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
package org.apache.gobblin.metadata.types;

import java.util.HashMap;
import java.util.Map;


/**
 * Represents a collection of global and record-level metadata that can be
 * attached to a record as it flows through a Gobblin pipeline.
 */
public class Metadata {
  private GlobalMetadata globalMetadata;
  private Map<String, Object> recordMetadata;

  public Metadata() {
    globalMetadata = new GlobalMetadata();
    recordMetadata = new HashMap<>();
  }

  public GlobalMetadata getGlobalMetadata() {
    return globalMetadata;
  }

  public Map<String, Object> getRecordMetadata() {
    return recordMetadata;
  }
}
