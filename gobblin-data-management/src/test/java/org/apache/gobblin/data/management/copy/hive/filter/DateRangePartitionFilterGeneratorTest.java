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

package org.apache.gobblin.data.management.copy.hive.filter;

import java.util.Properties;
import org.apache.gobblin.data.management.copy.hive.PartitionFilterGenerator;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = { "SystemTimeTests"})
public class DateRangePartitionFilterGeneratorTest {

  @BeforeMethod
  public void setUp()
      throws Exception {
    DateTimeUtils.setCurrentMillisFixed(new DateTime(2016,3,15,10,15).getMillis());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testInitialization() {
    PartitionFilterGenerator filter = GobblinConstructorUtils.invokeConstructor(PartitionFilterGenerator.class,
        DateRangePartitionFilterGenerator.class.getName(), System.getProperties());
    Assert.assertTrue(filter instanceof DateRangePartitionFilterGenerator);
  }
  @Test
  public void test() {
    doTest("datePartition", "2020-01-01", "2020-01-10", "datePartition between \"2020-01-01\" and \"2020-01-10\"");
  }


  private void doTest(String column, String sDate, String eDate, String expected) {
    Properties properties = new Properties();
    properties.put(DateRangePartitionFilterGenerator.PARTITION_COLUMN, column);
    properties.put(DateRangePartitionFilterGenerator.START_DATE, sDate);
    properties.put(DateRangePartitionFilterGenerator.END_DATE, eDate);

    PartitionFilterGenerator filterImpl = GobblinConstructorUtils.invokeConstructor(PartitionFilterGenerator.class,
        DateRangePartitionFilterGenerator.class.getName(), properties);

    Assert.assertEquals(filterImpl.getFilter(null), expected);
  }

}
