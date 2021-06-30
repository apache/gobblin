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

package org.apache.gobblin.troubleshooter;

import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooter;
import org.apache.gobblin.runtime.troubleshooter.AutomaticTroubleshooterFactory;
import org.apache.gobblin.util.ConfigUtils;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class AutomaticTroubleshooterTest {
  private final static Logger log = LogManager.getLogger(AutoTroubleshooterLogAppenderTest.class);

  @Test
  public void canCollectAndRefineIssues()
      throws Exception {
    Properties properties = new Properties();
    AutomaticTroubleshooter troubleshooter =
        AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(properties));
    try {
      troubleshooter.start();
      log.warn("Test warning");

      troubleshooter.refineIssues();
      troubleshooter.logIssueSummary();

      String summaryMessage = troubleshooter.getIssueSummaryMessage();
      assertTrue(summaryMessage.contains("Test warning"));

      String detailedMessage = troubleshooter.getIssueDetailsMessage();
      assertTrue(detailedMessage.contains("Test warning"));

      EventSubmitter eventSubmitter = mock(EventSubmitter.class);
      troubleshooter.reportJobIssuesAsEvents(eventSubmitter);

      assertEquals(1, troubleshooter.getIssueRepository().getAll().size());
      verify(eventSubmitter, times(1)).submit((GobblinEventBuilder) any());
    } finally {
      troubleshooter.stop();
    }
  }

  @Test
  public void canDisable()
      throws Exception {
    Properties properties = new Properties();
    properties.put(ConfigurationKeys.TROUBLESHOOTER_DISABLED, "true");
    AutomaticTroubleshooter troubleshooter =
        AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(properties));
    try {
      troubleshooter.start();
      log.warn("Test warning");

      troubleshooter.refineIssues();
      troubleshooter.logIssueSummary();
      EventSubmitter eventSubmitter = mock(EventSubmitter.class);
      troubleshooter.reportJobIssuesAsEvents(eventSubmitter);

      assertEquals(0, troubleshooter.getIssueRepository().getAll().size());
      verify(eventSubmitter, never()).submit((GobblinEventBuilder) any());
    } finally {
      troubleshooter.stop();
    }
  }

  @Test
  public void canDisableEventReporter()
      throws Exception {
    Properties properties = new Properties();
    properties.put(ConfigurationKeys.TROUBLESHOOTER_DISABLED, "false");
    properties.put(ConfigurationKeys.TROUBLESHOOTER_DISABLE_EVENT_REPORTING, "true");
    AutomaticTroubleshooter troubleshooter =
        AutomaticTroubleshooterFactory.createForJob(ConfigUtils.propertiesToConfig(properties));
    try {
      troubleshooter.start();

      log.warn("Test warning");

      troubleshooter.refineIssues();
      troubleshooter.logIssueSummary();
      EventSubmitter eventSubmitter = mock(EventSubmitter.class);
      troubleshooter.reportJobIssuesAsEvents(eventSubmitter);

      assertEquals(1, troubleshooter.getIssueRepository().getAll().size());
      verify(eventSubmitter, never()).submit((GobblinEventBuilder) any());
    } finally {
      troubleshooter.stop();
    }
  }
}