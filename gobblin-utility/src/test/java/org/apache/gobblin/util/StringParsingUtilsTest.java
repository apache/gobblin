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

package org.apache.gobblin.util;

import org.testng.Assert;
import org.testng.annotations.Test;


public class StringParsingUtilsTest {

  @Test
  public void testHumanReadableToByteCount()
      throws Exception {
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("10"), 10L);
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("1k"), 1024L);
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("1m"), 1048576L);
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("1g"), 1073741824L);
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("1t"), 1099511627776L);

    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("1K"), 1024L);
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("1kb"), 1024L);
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("1KB"), 1024L);

    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("2k"), 2048L);
    Assert.assertEquals(StringParsingUtils.humanReadableToByteCount("2.5k"), 2560L);
  }
}
