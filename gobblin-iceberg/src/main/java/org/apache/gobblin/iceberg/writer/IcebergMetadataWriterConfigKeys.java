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

public class IcebergMetadataWriterConfigKeys {

  public static final String ICEBERG_COMPLETENESS_ENABLED = "iceberg.completeness.enabled";
  public static final boolean DEFAULT_ICEBERG_COMPLETENESS = false;
  public static final String ICEBERG_COMPLETENESS_WHITELIST = "iceberg.completeness.whitelist";
  public static final String ICEBERG_COMPLETENESS_BLACKLIST = "iceberg.completeness.blacklist";
  public static final String COMPLETION_WATERMARK_KEY = "completionWatermark";
  public static final String COMPLETION_WATERMARK_TIMEZONE_KEY = "completionWatermarkTimezone";
  public static final long DEFAULT_COMPLETION_WATERMARK = -1L;
  public static final String TIME_ZONE_KEY = "iceberg.completeness.timezone";
  public static final String DEFAULT_TIME_ZONE = "America/Los_Angeles";
  public static final String DATEPARTITION_FORMAT = "yyyy-MM-dd-HH";
  public static final String NEW_PARTITION_KEY = "iceberg.completeness.add.partition";
  public static final String DEFAULT_NEW_PARTITION = "late";
  public static final String NEW_PARTITION_TYPE_KEY = "iceberg.completeness.add.partition.type";
  public static final String DEFAULT_PARTITION_COLUMN_TYPE = "string";
  public static final String TOPIC_NAME_KEY = "topic.name";
  public static final String AUDIT_CHECK_GRANULARITY = "iceberg.completeness.audit.check.granularity";
  public static final String DEFAULT_AUDIT_CHECK_GRANULARITY = "HOUR";
  public static final String ICEBERG_NEW_PARTITION_ENABLED = "iceberg.new.partition.enabled";
  public static final boolean DEFAULT_ICEBERG_NEW_PARTITION_ENABLED = false;
  public static final String ICEBERG_NEW_PARTITION_WHITELIST = "iceberg.new.partition.whitelist";
  public static final String ICEBERG_NEW_PARTITION_BLACKLIST = "iceberg.new.partition.blacklist";


}
