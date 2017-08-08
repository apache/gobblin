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
package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Collection;

import org.codehaus.jackson.map.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


/**
 * Metrics that can be stored in workUnitState by filesystem based writers.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Slf4j
public class FsWriterMetrics {
  // Note: all of these classes are @NoArgsConstructor because Jackson requires
  // an empty object to construct from JSON
  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Getter
  public static class FileInfo {
    String fileName;
    long numRecords;
  }

  String writerId;
  PartitionIdentifier partitionInfo;
  Collection<FileInfo> fileInfos;

  /**
   * Serialize this class to Json
   */
  public String toJson() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (IOException e) {
      log.error("IOException serializing FsWriterMetrics as JSON! Returning no metrics", e);
      return "{}";
    }
  }

  /**
   * Instantiate an object of this class from its JSON representation
   */
  public static FsWriterMetrics fromJson(String in) throws IOException {
    return new ObjectMapper().readValue(in, FsWriterMetrics.class);
  }
}
