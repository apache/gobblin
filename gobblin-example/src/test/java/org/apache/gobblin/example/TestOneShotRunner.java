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
package org.apache.gobblin.example;

import java.net.URL;

import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.example.generic.OneShotRunner;
import org.apache.gobblin.runtime.api.Configurable;


public class TestOneShotRunner {

  @Test
  public void testConfiguration() {
    OneShotRunner runner = new OneShotRunner();
    URL appConfResource = getClass().getClassLoader().getResource("appConf.conf");
    URL baseConfResource = getClass().getClassLoader().getResource("baseConf.conf");
    runner.appConf("file://" + appConfResource.getFile());
    runner.baseConf("file://" + baseConfResource.getFile());
    Assert.assertEquals(runner.getJobFile().get(), new Path("file://" + appConfResource.getPath()));
    Configurable resolvedSysConfig = runner.getSysConfig();
    Assert.assertEquals(resolvedSysConfig.getConfig().getString("test.key1"), "value1");
    Assert.assertEquals(resolvedSysConfig.getConfig().getString("test.key2"), "value2");
  }
}
