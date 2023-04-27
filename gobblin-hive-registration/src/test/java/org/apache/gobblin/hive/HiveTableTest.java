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

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import junit.framework.Assert;

import org.apache.gobblin.configuration.State;

public class HiveTableTest {

  @Test
  public void testPopulateFieldTypeCasting() throws Exception {
    // Test one property of each type in HiveRegistrationUnit storageProps

    State props = new State();
    Long lastAccessTime = System.currentTimeMillis();
    props.setProp(HiveConstants.LAST_ACCESS_TIME, String.valueOf(lastAccessTime));
    State storageProps = new State();
    storageProps.setProp(HiveConstants.LOCATION, "/tmp");
    storageProps.setProp(HiveConstants.COMPRESSED, "false");
    storageProps.setProp(HiveConstants.NUM_BUCKETS, "1");
    storageProps.setProp(HiveConstants.BUCKET_COLUMNS, "col1, col2");
    HiveTable.Builder builder = new HiveTable.Builder();
    builder.withTableName("tableName");
    builder.withDbName("dbName");
    builder.withProps(props);
    builder.withStorageProps(storageProps);

    HiveTable hiveTable = builder.build();

    Assert.assertEquals(hiveTable.getLastAccessTime().get().longValue(), lastAccessTime.longValue());
    Assert.assertEquals(hiveTable.getLocation().get(), "/tmp");
    Assert.assertEquals(hiveTable.isCompressed.get().booleanValue(), false);
    Assert.assertEquals(hiveTable.getNumBuckets().get().intValue(), 1);
    List<String> bucketColumns = new ArrayList<>();
    bucketColumns.add("col1");
    bucketColumns.add("col2");
    Assert.assertEquals(hiveTable.getBucketColumns().get().get(0), bucketColumns.get(0));
    Assert.assertEquals(hiveTable.getBucketColumns().get().get(1), bucketColumns.get(1));
  }
}
