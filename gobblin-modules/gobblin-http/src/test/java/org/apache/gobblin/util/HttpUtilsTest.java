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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

import com.typesafe.config.ConfigFactory;

import junit.framework.Assert;

import org.apache.gobblin.http.ResponseStatus;
import org.apache.gobblin.http.StatusType;
import org.apache.gobblin.utils.HttpConstants;
import org.apache.gobblin.utils.HttpUtils;


@Test
public class HttpUtilsTest {

  public void testGetErrorCodeWhitelist() {
    Map<String, String> map = new HashMap<>();
    map.put(HttpConstants.ERROR_CODE_WHITELIST, "303, 3xx, 303, 4XX");
    Set<String> whitelist = HttpUtils.getErrorCodeWhitelist(ConfigFactory.parseMap(map));
    Assert.assertTrue(whitelist.size() == 3);
    Assert.assertTrue(whitelist.contains("303"));
    Assert.assertTrue(whitelist.contains("3xx"));
    Assert.assertTrue(whitelist.contains("4xx"));
    Assert.assertFalse(whitelist.contains("4XX"));
  }

  public void testUpdateStatusType() {
    Set<String> errorCodeWhitelist = new HashSet<>();
    ResponseStatus status = new ResponseStatus(StatusType.OK);

    HttpUtils.updateStatusType(status, 303, errorCodeWhitelist);
    // Client error without whitelist
    Assert.assertTrue(status.getType() == StatusType.CLIENT_ERROR);

    errorCodeWhitelist.add("303");
    HttpUtils.updateStatusType(status, 303, errorCodeWhitelist);
    // Continue with whitelist
    Assert.assertTrue(status.getType() == StatusType.CONTINUE);

    errorCodeWhitelist.clear();
    errorCodeWhitelist.add("3xx");
    HttpUtils.updateStatusType(status, 303, errorCodeWhitelist);
    // Continue with whitelist
    Assert.assertTrue(status.getType() == StatusType.CONTINUE);

    HttpUtils.updateStatusType(status, 404, errorCodeWhitelist);
    // Client error without whitelist
    Assert.assertTrue(status.getType() == StatusType.CLIENT_ERROR);

    errorCodeWhitelist.add("4xx");
    HttpUtils.updateStatusType(status, 404, errorCodeWhitelist);
    // Continue with whitelist
    Assert.assertTrue(status.getType() == StatusType.CONTINUE);

    HttpUtils.updateStatusType(status, 505, errorCodeWhitelist);
    // Server error without whitelist
    Assert.assertTrue(status.getType() == StatusType.SERVER_ERROR);

    errorCodeWhitelist.add("5xx");
    HttpUtils.updateStatusType(status, 505, errorCodeWhitelist);
    // Continue with whitelist
    Assert.assertTrue(status.getType() == StatusType.CONTINUE);
  }
}
