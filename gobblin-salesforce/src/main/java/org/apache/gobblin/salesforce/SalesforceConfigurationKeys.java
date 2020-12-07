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

package org.apache.gobblin.salesforce;

/**
 * SalesforceConfigurationKeys
 */
public final class SalesforceConfigurationKeys {
  private SalesforceConfigurationKeys() {
  }
  public static final String SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED =
      "source.querybased.salesforce.is.soft.deletes.pull.disabled";

  // bulk api retry sleep duration for avoid resource consuming peak.
  public static final String RETRY_EXCEED_QUOTA_INTERVAL = "salesforce.retry.exceedQuotaInterval";
  public static final long RETRY_EXCEED_QUOTA_INTERVAL_DEFAULT = 5 * 60 * 1000;

  public static final String RETRY_INTERVAL = "salesforce.retry.interval";
  public static final long RETRY_INTERVAL_DEFAULT = 1 * 60 * 1000;

  // pk-chunking
  public static final String BULK_TEST_JOB_ID = "salesforce.bulk.testJobId";
  public static final String BULK_TEST_BATCH_ID_LIST = "salesforce.bulk.testBatchIds";
  public static final String SALESFORCE_PARTITION_TYPE = "salesforce.partitionType";
  public static final String PK_CHUNKING_JOB_ID = "__salesforce.job.id"; // don't use in ini config
  public static final String PK_CHUNKING_BATCH_RESULT_ID_PAIRS = "__salesforce.batch.result.id.pairs"; // don't use in ini config
}
