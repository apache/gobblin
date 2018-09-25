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


public class HiveJdbcConnectorTest {

  @Test
  public void testChoppedStatementNoLineChange() {
    String example1 = "This is\na test";
    String example2 = "This is\r\na test\nstring";
    String expected1 = "This is a test";
    String expected2 = "This is a test string";
    Assert.assertEquals(HiveJdbcConnector.choppedStatementNoLineChange(example1), expected1);
    Assert.assertEquals(HiveJdbcConnector.choppedStatementNoLineChange(example2), expected2);

    // Generate a random string longer than 1000 charaters
    int iter = 501;
    StringBuilder exampleExpected = new StringBuilder();
    StringBuilder exampleResult = new StringBuilder();
    while (iter > 0) {
      exampleExpected.append("a ");
      exampleResult.append("a\n");
      iter -- ;
    }
    String expected = exampleExpected.toString().substring(0, 1000) + "...... (2 characters omitted)";
    Assert.assertEquals(HiveJdbcConnector.choppedStatementNoLineChange(exampleResult.toString()), expected);
  }
}