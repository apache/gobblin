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

public final class SalesforceConfigurationKeys {
  private SalesforceConfigurationKeys() {
  }
  public static final String SOURCE_QUERYBASED_SALESFORCE_IS_SOFT_DELETES_PULL_DISABLED =
      "source.querybased.salesforce.is.soft.deletes.pull.disabled";
  public static final int DEFAULT_FETCH_RETRY_LIMIT = 5;
  public static final String BULK_API_USE_QUERY_ALL = "salesforce.bulkApiUseQueryAll";

  // pk-chunking
  public static final String PK_CHUNKING_TEST_BATCH_ID_LIST = "salesforce.pkChunking.testBatchIdList";
  public static final String PK_CHUNKING_TEST_JOB_ID = "salesforce.pkChunking.testJobId";
  public static final String SALESFORCE_PARTITION_TYPE = "salesforce.partitionType";
  public static final String PARTITION_PK_CHUNKING_SIZE = "salesforce.partition.pkChunkingSize";
  public static final String PK_CHUNKING_JOB_ID = "_salesforce.job.id";
  public static final String PK_CHUNKING_BATCH_RESULT_IDS = "_salesforce.result.ids";
  public static final int MAX_PK_CHUNKING_SIZE = 250_000; // this number is from SFDC's doc - https://tinyurl.com/ycjvgwv2
  public static final int MIN_PK_CHUNKING_SIZE = 20_000;
  public static final int DEFAULT_PK_CHUNKING_SIZE = 250_000; // default to max for saving request quota
}


