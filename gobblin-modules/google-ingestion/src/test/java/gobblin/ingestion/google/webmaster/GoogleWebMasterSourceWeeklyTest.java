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

package gobblin.ingestion.google.webmaster;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(groups = {"gobblin.source.extractor.extract.google.webmaster"})
public class GoogleWebMasterSourceWeeklyTest {
  private static DateTimeFormatter _dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
  private static DateTimeFormatter _hmsFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");

  @Test
  public void testGetTaskRangeForMonday() throws Exception {
    long startingDate = 20161121162339L;
    int oneDay = 1000000;
    int i = 0;
    for (; i < 6; ++i) {
      long lowWatermark = startingDate + oneDay * i;
      DateTime date = _hmsFormatter.parseDateTime(Long.toString(lowWatermark));
      System.out.println(String.format("Date: %s, Day of week: %d", _dateFormatter.print(date), date.getDayOfWeek()));
      Pair<DateTime, DateTime> taskRange = GoogleWebMasterSourceWeekly.getTaskRange(lowWatermark);
      Assert.assertEquals(_dateFormatter.print(taskRange.getLeft()), "2016-11-11");
      Assert.assertEquals(_dateFormatter.print(taskRange.getRight()), "2016-11-17");
    }

    long lowWatermark = startingDate + oneDay * i;
    DateTime date = _hmsFormatter.parseDateTime(Long.toString(lowWatermark));
    System.out.println(String.format("Date: %s, Day of week: %d", _dateFormatter.print(date), date.getDayOfWeek()));
    Pair<DateTime, DateTime> taskRange = GoogleWebMasterSourceWeekly.getTaskRange(lowWatermark);
    Assert.assertEquals(_dateFormatter.print(taskRange.getLeft()), "2016-11-18");
    Assert.assertEquals(_dateFormatter.print(taskRange.getRight()), "2016-11-24");
  }
}