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

package org.apache.gobblin.source;

import java.io.IOException;

import org.joda.time.Duration;
import org.joda.time.chrono.ISOChronology;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Tests for {@link PartitionAwareFileRetrieverUtils}*/
public class PartitionAwareFileRetrieverUtilsTest {

  @Test(dataProvider = "validLookbackTimes")
  public void testValidLookbackTime(String lookBackTime, long expectedMillis) throws IOException {
    Duration expectedDuration = new Duration(expectedMillis);
    Duration actualDuration = PartitionAwareFileRetrieverUtils.getLookbackTimeDuration(lookBackTime);
    Assert.assertEquals(expectedDuration, actualDuration);
  }

  @Test(dataProvider = "invalidLookbackTimes", expectedExceptions = IOException.class)
  public void testInvalidLookbackTime(String lookBackTime) throws IOException {
    PartitionAwareFileRetrieverUtils.getLookbackTimeDuration(lookBackTime);
  }

  @DataProvider(name = "validLookbackTimes")
  public Object[][] validLookbackTimes() {
    return new Object[][] {
        {"5d", 5 * ISOChronology.getInstance().days().getUnitMillis()},
        {"10h", 10 * ISOChronology.getInstance().hours().getUnitMillis()},
        {"30m", 30 * ISOChronology.getInstance().minutes().getUnitMillis()}
    };
  }

  @DataProvider(name = "invalidLookbackTimes")
  public String[][] invalidLookbackTimes() {
    return new String[][] {
        {"5x"}, // Invalid format
        {"30z"}, // Invalid format
        {"xd"}, // Invalid number
        {"yh"}, // Invalid number
        {"zm"}  // Invalid number
    };
  }

}
