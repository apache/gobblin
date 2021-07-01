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

package org.apache.gobblin.runtime.troubleshooter;

import java.util.Properties;

import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.ConfigUtils;

import static org.junit.Assert.assertTrue;


public class AutomaticTroubleshooterFactoryTest {

  @Test
  public void willGetNoopTroubleshooterByDefault() {
    // This test project does not reference gobblin-troubleshooter module, so we should get a noop-instance
    // of troubleshooter. See the main AutomaticTroubleshooterFactory class for details.
    Properties properties = new Properties();
    AutomaticTroubleshooter troubleshooter =
        AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(properties));

    assertTrue(troubleshooter instanceof NoopAutomaticTroubleshooter);
  }

  @Test
  public void willGetNoopTroubleshooterWhenDisabled() {
    Properties properties = new Properties();
    properties.put(ConfigurationKeys.TROUBLESHOOTER_DISABLED, "true");
    AutomaticTroubleshooter troubleshooter =
        AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(properties));

    assertTrue(troubleshooter instanceof NoopAutomaticTroubleshooter);
  }
}