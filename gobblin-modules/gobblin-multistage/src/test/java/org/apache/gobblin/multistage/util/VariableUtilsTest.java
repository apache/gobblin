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

package org.apache.gobblin.multistage.util;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.UnsupportedEncodingException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.gobblin.multistage.util.VariableUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
public class VariableUtilsTest {
  private final static String TEMPLATE = "{\"%s\":\"%s\"}";
  private final static String TEST_DATE_STRING = "2019-11-01 12:00:00";
  private final static String START_DATE_NAME = "startDate";
  private final static String END_DATE_NAME = "endDate";
  private Gson gson;
  private JsonObject parameters;

  @BeforeClass
  public void setUp() {
    gson = new Gson();
  }

  @Test
  void testReplaceWithTracking() throws UnsupportedEncodingException {
    String template = "\\'{{Activity.CreatedAt}}\\' >= \\'{{startDate}}\\' and \\'{{Activity.CreatedAt}}\\' < \\'{{endDate}}\\'";
    JsonObject parameters = new JsonObject();
    parameters.addProperty("startDate", "2019-11-01 12:00:00");
    parameters.addProperty("endDate", "2019-11-02 12:00:00");
    String expected = "\\'{{Activity.CreatedAt}}\\' >= \\'2019-11-01 12:00:00\\' and \\'{{Activity.CreatedAt}}\\' < \\'2019-11-02 12:00:00\\'";
    Assert.assertEquals(VariableUtils.replaceWithTracking(template, parameters, false).getKey(), expected);
    Assert.assertEquals(VariableUtils.replaceWithTracking(template, parameters).getKey(), expected);

    expected = "\\'{{Activity.CreatedAt}}\\' >= \\'2019-11-01+12%3A00%3A00\\' and \\'{{Activity.CreatedAt}}\\' < \\'2019-11-02+12%3A00%3A00\\'";
    Assert.assertEquals(VariableUtils.replaceWithTracking(template, parameters, true).getKey(), expected);
  }

  /**
   * Test: parameters contains value for placeholders in template
   * Expected: placeholder replaced
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testReplaceWithTrackingII() throws UnsupportedEncodingException {
    parameters = new JsonObject();
    parameters.addProperty(START_DATE_NAME, TEST_DATE_STRING);
    Assert.assertEquals(VariableUtils.replace(gson.fromJson(String.format(TEMPLATE, START_DATE_NAME, "{{startDate}}"), JsonObject.class), parameters).toString(),
        String.format(TEMPLATE, START_DATE_NAME, TEST_DATE_STRING));
  }

  /**
   * Test: parameters doesn't contains value for placeholders in template
   * Expected: placeholder not replaced
   * @throws UnsupportedEncodingException
   */
  @Test
  public void testReplace() throws UnsupportedEncodingException {
    String expected = String.format(String.format(TEMPLATE, START_DATE_NAME, "{{startDate}}"));
    parameters = new JsonObject();
    parameters.addProperty(END_DATE_NAME, TEST_DATE_STRING);
    Assert.assertEquals(VariableUtils.replaceWithTracking(expected, parameters, false),
        new ImmutablePair<>(expected, gson.fromJson(
            String.format(TEMPLATE, END_DATE_NAME, TEST_DATE_STRING), JsonObject.class)));
  }
}
