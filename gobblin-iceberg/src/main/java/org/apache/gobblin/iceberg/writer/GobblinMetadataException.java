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

package org.apache.gobblin.iceberg.writer;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.gobblin.hive.HiveRegistrationUnit;
import org.apache.gobblin.metadata.OperationType;


public class GobblinMetadataException extends IOException {
  public String datasetPath;
  public String dbName;
  public String tableName;
  public String GMCETopicPartition;
  public long highWatermark;
  public long lowWatermark;
  public List<String> failedWriters;
  public OperationType operationType;
  public Set<String> addedPartitionValues;
  public Set<String> droppedPartitionValues;
  public List<HiveRegistrationUnit.Column> partitionKeys;

  GobblinMetadataException(String datasetPath, String dbName, String tableName, String GMCETopicPartition, long lowWatermark, long highWatermark,
      List<String> failedWriters, OperationType operationType, List<HiveRegistrationUnit.Column> partitionKeys, Exception exception) {
    super(String.format("failed to flush table %s, %s", dbName, tableName), exception);
    this.datasetPath = datasetPath;
    this.dbName = dbName;
    this.tableName = tableName;
    this.GMCETopicPartition = GMCETopicPartition;
    this.highWatermark = highWatermark;
    this.lowWatermark = lowWatermark;
    this.failedWriters = failedWriters;
    this.operationType = operationType;
    this.addedPartitionValues = new HashSet<>();
    this.droppedPartitionValues = new HashSet<>();
    this.partitionKeys = partitionKeys;
  }
}
