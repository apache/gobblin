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

package org.apache.gobblin.runtime.messaging;

public class DynamicWorkUnitConfigKeys {
  public static final String DYNAMIC_WORKUNIT_PREFIX = "gobblin.dynamic.workunit.";

  public static final String DYNAMIC_WORKUNIT_HDFS_CHANNEL_NAME = DYNAMIC_WORKUNIT_PREFIX + "hdfs";
  public static final String DYNAMIC_WORKUNIT_HDFS_PREFIX = DYNAMIC_WORKUNIT_PREFIX + "hdfs.";

  public static final String DYNAMIC_WORKUNIT_HDFS_PATH = DYNAMIC_WORKUNIT_HDFS_PREFIX + "path";
  public static final String DYNAMIC_WORKUNIT_HDFS_POLLING_RATE_MILLIS = DYNAMIC_WORKUNIT_HDFS_PREFIX + "pollingRate";
  public static final long DYNAMIC_WORKUNIT_HDFS_DEFAULT_POLLING_RATE = 30000;

  public static final String DYNAMIC_WORKUNIT_HDFS_FOLDER = "dynamic-workunit";
}