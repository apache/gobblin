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

package org.apache.gobblin.hive;

import org.apache.gobblin.annotation.Alpha;


/**
 * A class containing constants used in {@link HiveTable} and {@link HivePartition}.
 */
@Alpha
public class HiveConstants {

  private HiveConstants() {
  }

  /**
   * Table and partition properties
   */
  public static final String CREATE_TIME = "create.time";
  public static final String LAST_ACCESS_TIME = "last.access.time";
  public static final String SCHEMA_TIMESTAMP = "schema.timestamp";

  /**
   * Table properties
   */
  public static final String OWNER = "owner";
  public static final String TABLE_TYPE = "table.type";
  public static final String RETENTION = "retention";

  /**
   * Storage properties
   */
  public static final String LOCATION = "location";
  public static final String INPUT_FORMAT = "input.format";
  public static final String OUTPUT_FORMAT = "output.format";
  public static final String COMPRESSED = "compressed";
  public static final String NUM_BUCKETS = "num.buckets";
  public static final String BUCKET_COLUMNS = "bucket.columns";
  public static final String STORED_AS_SUB_DIRS = "stored.as.sub.dirs";

  /**
   * SerDe properties
   */
  public static final String SERDE_TYPE = "serde.type";
}
