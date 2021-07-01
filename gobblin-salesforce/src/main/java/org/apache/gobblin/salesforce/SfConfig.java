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

import java.util.Properties;

import org.apache.gobblin.typedconfig.Default;
import org.apache.gobblin.typedconfig.Key;
import org.apache.gobblin.typedconfig.compiletime.IntRange;

public class SfConfig extends QueryBasedSourceConfig {
  public SfConfig(Properties prop) {
    super(prop);
  }

  @Key("salesforce.partition.pkChunkingSize")@Default("250000")@IntRange({20_000, 250_000})
  public int pkChunkingSize;

  @Key("salesforce.bulkApiUseQueryAll")@Default("false")
  public boolean bulkApiUseQueryAll;

  @Key("salesforce.retry.interval")@Default("60000")
  public int retryInterval;

  @Key("salesforce.fetchRetryLimit")@Default("5")
  public int fetchRetryLimit;

  @Key("salesforce.retry.exceedQuotaInterval")@Default("300000")
  public int retryExceedQuotaInterval;

  @Key("sf.rest.api.retryLimit")@Default("3")
  public int restApiRetryLimit;

  @Key("sf.rest.api.retryInterval")@Default("10000") // 10 seconds
  public int restApiRetryInterval;

  // it is for test. if true, it will only execute partition part and stop.
  @Key("sf.test.partitionOnly")@Default("false")
  public boolean partitionOnly;

}
