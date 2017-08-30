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

package org.apache.gobblin.data.management.conversion.hive.task;

import java.util.Map;

import org.junit.Test;
import org.testng.Assert;
import org.testng.collections.Maps;

import com.google.common.base.Optional;

public class HiveConverterUtilsTest {
  private final String inputDbName = "testdb";
  private final String inputTableName = "testtable";
  private final String outputDatabaseName = "testdb2";
  private final String outputTableName = "testtable2";

  @Test
  public void copyTableQueryTest() throws Exception {
    Map<String, String> partitionsDMLInfo = Maps.newHashMap();
    String partitionName = "datepartition";
    String partitionValue = "2017-07-15-08";

    partitionsDMLInfo.put(partitionName, partitionValue);
    String expectedQuery = "INSERT OVERWRITE TABLE `" + outputDatabaseName + "`.`" + outputTableName + "` \n"
        + "PARTITION (`" + partitionName + "`) \n" + "SELECT * FROM `" + inputDbName + "`.`" + inputTableName + "` WHERE "
        + "`" + partitionName + "`='" + partitionsDMLInfo.get(partitionName) + "'";

    String actualQuery = HiveConverterUtils.generateTableCopy(inputTableName,
        outputTableName, inputDbName, outputDatabaseName, Optional.of(partitionsDMLInfo));
    Assert.assertEquals(expectedQuery, actualQuery);
  }
}
