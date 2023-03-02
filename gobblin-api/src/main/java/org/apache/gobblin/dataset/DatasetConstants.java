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

package org.apache.gobblin.dataset;

public class DatasetConstants {
  /** Platforms */
  public static final String PLATFORM_FILE = "file";
  public static final String PLATFORM_HDFS = "hdfs";
  public static final String PLATFORM_KAFKA = "kafka";
  public static final String PLATFORM_HIVE = "hive";
  public static final String PLATFORM_SALESFORCE = "salesforce";
  public static final String PLATFORM_MYSQL = "mysql";
  public static final String PLATFORM_ICEBERG = "iceberg";

  /** Common metadata */
  public static final String BRANCH = "branch";

  /** File system metadata */
  public static final String FS_URI = "fsUri";

  /** Kafka metadata */
  public static final String BROKERS = "brokers";

  /** JDBC metadata */
  public static final String CONNECTION_URL = "connectionUrl";

  /** FileSystem scheme and location */
  public static final String FS_SCHEME = "fsScheme";
  public static final String FS_LOCATION = "fsLocation";
}
