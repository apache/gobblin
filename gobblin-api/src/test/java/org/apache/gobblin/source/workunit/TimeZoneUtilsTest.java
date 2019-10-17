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

package org.apache.gobblin.source.workunit;

import java.time.ZoneId;
import java.util.TimeZone;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TimeZoneUtilsTest {
  @Test
  public void testConfigurableTimeZone()
      throws Exception {
    SourceState state = new SourceState();
    state.setProp(ConfigurationKeys.EXTRACT_ID_TIME_ZONE, "America/Los_Angeles");
    Extract extract = new Extract(state, Extract.TableType.APPEND_ONLY, "random", "table");
    Assert.assertEquals(extract.getTimeZoneHelper(state).toTimeZone(),
        TimeZone.getTimeZone(ZoneId.of("America/Los_Angeles")));

    state.removeProp(ConfigurationKeys.EXTRACT_ID_TIME_ZONE);
    extract = new Extract(state, Extract.TableType.APPEND_ONLY, "random", "table");
    Assert.assertEquals(extract.getTimeZoneHelper(state), DateTimeZone.UTC);
  }
}
