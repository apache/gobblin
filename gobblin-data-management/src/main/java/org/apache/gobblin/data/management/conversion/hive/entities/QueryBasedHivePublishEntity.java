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
package gobblin.data.management.conversion.hive.entities;

import java.util.List;
import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Entity to carry Hive queries to publish table and partitions from writer to publisher.
 * This entity also holds references to directories to be moved or deleted while publishing.
 */
@ToString
@EqualsAndHashCode
@Getter
public class QueryBasedHivePublishEntity {
  // Hive queries to execute to publish table and / or partitions.
  private List<String> publishQueries;
  // Directories to move: key is source, value is destination.
  private Map<String, String> publishDirectories;

  // Hive queries to cleanup after publish step.
  private List<String> cleanupQueries;
  // Directories to delete after publish step.
  private List<String> cleanupDirectories;

  public QueryBasedHivePublishEntity() {
    this.publishQueries = Lists.newArrayList();
    this.publishDirectories = Maps.newHashMap();

    this.cleanupQueries = Lists.newArrayList();
    this.cleanupDirectories = Lists.newArrayList();
  }
}
