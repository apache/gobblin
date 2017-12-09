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
package org.apache.gobblin.service.modules.orchestration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import lombok.extern.slf4j.Slf4j;

import org.testng.Assert;
import org.testng.annotations.Test;


@Slf4j
@Test(groups = { "org.apache.gobblin.service.modules.orchestration" })
public class AzkabanAjaxAPIClientTest {

  @Test
  public void testCurrentTimeWithinWindow()
      throws ParseException {
    // Generate a window encapsulating the current time
    int windowStartInHours = 2;
    int windowEndInHours = 5;
    int delayInMinutes = 0;

    // Get computed scheduled time
    String outputScheduledString =
        AzkabanAjaxAPIClient.getScheduledTimeInAzkabanFormat(windowStartInHours, windowEndInHours, delayInMinutes);

    // Verify that output schedule time is within window
    Assert.assertTrue(isWithinWindow(windowStartInHours, windowEndInHours, outputScheduledString));
  }

  @Test
  public void testCurrentTimeOutsideWindow()
      throws ParseException {
    // Current hour
    Calendar now = Calendar.getInstance();
    int currentHour = now.get(Calendar.HOUR_OF_DAY);

    // Generate a window NOT encapsulating the current time
    int windowStartInHours = currentHour > 10 ? 1 : 11;
    int windowEndInHours = currentHour > 10 ? 6 : 16;
    int delayInMinutes = 0;

    // Get computed scheduled time
    String outputScheduledString =
        AzkabanAjaxAPIClient.getScheduledTimeInAzkabanFormat(windowStartInHours, windowEndInHours, delayInMinutes);

    // Verify that output schedule time is within window
    Assert.assertTrue(isWithinWindow(windowStartInHours, windowEndInHours, outputScheduledString));
  }

  private boolean isWithinWindow(int windowStartInHours, int windowEndInHours, String outputScheduledString)
      throws ParseException {
    Calendar windowStart = Calendar.getInstance();
    windowStart.set(Calendar.HOUR_OF_DAY, windowStartInHours);
    windowStart.set(Calendar.MINUTE, 0);
    windowStart.set(Calendar.SECOND, 0);

    Calendar windowEnd = Calendar.getInstance();
    windowEnd.set(Calendar.HOUR_OF_DAY, windowEndInHours);
    windowEnd.set(Calendar.MINUTE, 0);
    windowEnd.set(Calendar.SECOND, 0);

    Date outputDate = new SimpleDateFormat("hh,mm,a,z").parse(outputScheduledString);
    Calendar receivedTime = Calendar.getInstance();
    receivedTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(new SimpleDateFormat("HH").format(outputDate)));
    receivedTime.set(Calendar.MINUTE, Integer.parseInt(new SimpleDateFormat("mm").format(outputDate)));

    log.info("Window start time is: " + new SimpleDateFormat("MM/dd/yyyy hh,mm,a,z").format(windowStart.getTime()));
    log.info("Window end time is: " + new SimpleDateFormat("MM/dd/yyyy hh,mm,a,z").format(windowEnd.getTime()));
    log.info("Output time is: " + new SimpleDateFormat("MM/dd/yyyy hh,mm,a,z").format(receivedTime.getTime()));

    return receivedTime.after(windowStart) && receivedTime.before(windowEnd);
  }
}