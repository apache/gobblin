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

public class IcebergMCEMetadataKeys {
  public static final String METRICS_NAMESPACE_ICEBERG_WRITER = "IcebergWriter";
  public static final String ICEBERG_COMMIT_EVENT_NAME = "IcebergMetadataCommitEvent";
  public static final String LAG_KEY_NAME = "endToEndLag";
  public static final String SNAPSHOT_KEY_NAME = "currentSnapshotId";
  public static final String MANIFEST_LOCATION = "currentManifestLocation";
  public static final String SNAPSHOT_INFORMATION_KEY_NAME = "currentSnapshotDetailedInformation";
  public static final String TABLE_KEY_NAME = "icebergTableName";
  public static final String DATABASE_KEY_NAME = "icebergDatabaseName";
  public static final String GMCE_TOPIC_NAME = "gmceTopicName";
  public static final String GMCE_TOPIC_PARTITION = "gmceTopicPartition";
  public static final String GMCE_HIGH_WATERMARK = "gmceHighWatermark";
  public static final String GMCE_LOW_WATERMARK = "gmceLowWatermark";
  public static final String DATASET_HDFS_PATH = "datasetHdfsPath";
  public static final String FAILURE_EVENT_DB_NAME = "databaseName";
  public static final String FAILURE_EVENT_TABLE_NAME = "tableName";

  private IcebergMCEMetadataKeys() {
  }
}
