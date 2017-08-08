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

package org.apache.gobblin.test;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Created by sdas on 7/27/16.
 */
public class ErrorManagerTest {


  @Test
  public void testErrorEvery()
  {
    Properties props = new Properties();
    props.setProperty(ErrorManager.ERROR_TYPE_CONFIGURATION_KEY, "nth");
    props.setProperty(ErrorManager.FLAKY_ERROR_EVERY_CONFIGURATION_KEY, "5");
    Config config = ConfigFactory.parseProperties(props);
    ErrorManager errorManager = new ErrorManager(config);
    for (int j = 0; j < 5; ++j) {
      for (int i = 0; i < 4; ++i) {
        Assert.assertEquals(errorManager.nextError(null), false, "Failed on " + i);
      }
      Assert.assertEquals(errorManager.nextError(null), true, "Failed on the last one");
    }

  }

  @Test
  public void testErrorRegex()
  {
    Properties props = new Properties();
    props.setProperty(ErrorManager.ERROR_TYPE_CONFIGURATION_KEY, "regex");
    props.setProperty(ErrorManager.FLAKY_ERROR_REGEX_PATTERN_KEY, ":index:0");
    Config config = ConfigFactory.parseProperties(props);
    ErrorManager<String> errorManager = new ErrorManager<>(config);

    Assert.assertEquals(errorManager.nextError(":index:0:seq:1"), true);
    Assert.assertEquals(errorManager.nextError(":index:1:seq:1"), false);
  }

}
