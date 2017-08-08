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

package org.apache.gobblin.util.request_allocation;


import java.util.Properties;

import org.apache.hadoop.hive.ql.metadata.Table;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.data.management.copy.hive.HiveDataset;
import org.apache.gobblin.data.management.partition.CopyableDatasetRequestor;


public class SimpleHiveDatasetTieringPrioritizerTest {
  @Test
  public void test() throws Exception {
    Properties props = new Properties();
    props.put(SimpleHiveDatasetTieringPrioritizer.TIER_KEY + ".0", "importantdb,somedb.importanttable");
    props.put(SimpleHiveDatasetTieringPrioritizer.TIER_KEY + ".1", "adb");

    SimpleHiveDatasetTieringPrioritizer prioritizer = new SimpleHiveDatasetTieringPrioritizer(props);
    Assert.assertEquals(prioritizer.compareRequestors(getRequestor("importantdb", "tablea"), getRequestor("importantdb", "tableb")), 0);
    Assert.assertEquals(prioritizer.compareRequestors(getRequestor("importantdb", "tablea"), getRequestor("otherdb", "tableb")), -1);
    Assert.assertEquals(prioritizer.compareRequestors(getRequestor("somedb", "importanttable"), getRequestor("importantdb", "tableb")), 0);
    Assert.assertEquals(prioritizer.compareRequestors(getRequestor("somedb", "importanttable"), getRequestor("somedb", "tableb")), -1);
    Assert.assertEquals(prioritizer.compareRequestors(getRequestor("adb", "tablea"), getRequestor("importantdb", "tableb")), 1);
    Assert.assertEquals(prioritizer.compareRequestors(getRequestor("adb", "tablea"), getRequestor("somedb", "tableb")), -1);
  }

  private CopyableDatasetRequestor getRequestor(String dbName, String tableName) {
    CopyableDatasetRequestor requestor = Mockito.mock(CopyableDatasetRequestor.class);
    HiveDataset dataset = Mockito.mock(HiveDataset.class);

    Table table = new Table(new org.apache.hadoop.hive.metastore.api.Table());
    table.setDbName(dbName);
    table.setTableName(tableName);

    Mockito.when(dataset.getTable()).thenReturn(table);
    Mockito.when(requestor.getDataset()).thenReturn(dataset);

    return requestor;
  }
}
