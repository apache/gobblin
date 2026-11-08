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

package org.apache.gobblin.azkaban;

import java.io.IOException;
import java.util.Properties;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.app.ApplicationLauncher;
import org.apache.gobblin.runtime.troubleshooter.IssueEventBuilder;
import org.apache.gobblin.yarn.GobblinYarnAppLauncher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


@Test
public class AzkabanGobblinYarnAppLauncherTest {

  private static Properties buildProps() {
    Properties props = new Properties();
    props.setProperty(ConfigurationKeys.FLOW_GROUP_KEY, "testGroup");
    props.setProperty(ConfigurationKeys.FLOW_NAME_KEY, "testFlow");
    props.setProperty(ConfigurationKeys.FLOW_EXECUTION_ID_KEY, "12345");
    props.setProperty(ConfigurationKeys.JOB_NAME_KEY, "testJob");
    props.setProperty(ConfigurationKeys.JOB_GROUP_KEY, "testGroup");
    return props;
  }

  private AzkabanGobblinYarnAppLauncher buildLauncher(GobblinYarnAppLauncher yarnLauncher,
      EventSubmitter eventSubmitter) throws IOException {
    ApplicationLauncher appLauncher = mock(ApplicationLauncher.class);
    return new AzkabanGobblinYarnAppLauncher("testJob", buildProps(), yarnLauncher, appLauncher, eventSubmitter);
  }

  @Test
  public void runCallsSubmitJobFailedEventOnLaunchFailure() throws Exception {
    GobblinYarnAppLauncher yarnLauncher = mock(GobblinYarnAppLauncher.class);
    EventSubmitter eventSubmitter = mock(EventSubmitter.class);
    TimingEvent mockTimer = mock(TimingEvent.class);
    Mockito.when(eventSubmitter.getTimingEvent(anyString())).thenReturn(mockTimer);

    IOException cause = new IOException("token refresh failed");
    doThrow(cause).when(yarnLauncher).launch();

    AzkabanGobblinYarnAppLauncher launcher = buildLauncher(yarnLauncher, eventSubmitter);

    try {
      launcher.run();
      Assert.fail("Expected IOException to propagate");
    } catch (IOException e) {
      Assert.assertEquals(e, cause);
    }

    // Only JOB_FAILED timing event (no JOB_COMPLETE for bootstrap failures)
    verify(eventSubmitter, times(1)).getTimingEvent(TimingEvent.LauncherTimings.JOB_FAILED);
    verify(mockTimer, times(1)).stop(any());

    // IssueEventBuilder submitted with bootstrap failure summary
    ArgumentCaptor<GobblinEventBuilder> captor = ArgumentCaptor.forClass(GobblinEventBuilder.class);
    verify(eventSubmitter, times(1)).submit(captor.capture());
    Assert.assertTrue(captor.getValue() instanceof IssueEventBuilder);
    IssueEventBuilder issueEvent = (IssueEventBuilder) captor.getValue();
    Assert.assertTrue(issueEvent.getIssue().getSummary().contains("YARN AM bootstrap failed"));
    Assert.assertTrue(issueEvent.getIssue().getSummary().contains("token refresh failed"));
  }

  @Test
  public void runDoesNotCallSubmitJobFailedEventOnSuccess() throws Exception {
    GobblinYarnAppLauncher yarnLauncher = mock(GobblinYarnAppLauncher.class);
    EventSubmitter eventSubmitter = mock(EventSubmitter.class);

    // launch() succeeds
    AzkabanGobblinYarnAppLauncher launcher = buildLauncher(yarnLauncher, eventSubmitter);
    launcher.run();

    verify(eventSubmitter, times(0)).getTimingEvent(anyString());
    verify(eventSubmitter, times(0)).submit(any(GobblinEventBuilder.class));
  }

  @Test
  public void issueEventContainsFlowMetadata() throws Exception {
    GobblinYarnAppLauncher yarnLauncher = mock(GobblinYarnAppLauncher.class);
    EventSubmitter eventSubmitter = mock(EventSubmitter.class);
    TimingEvent mockTimer = mock(TimingEvent.class);
    Mockito.when(eventSubmitter.getTimingEvent(anyString())).thenReturn(mockTimer);
    doThrow(new IOException("hdfs quota exceeded")).when(yarnLauncher).launch();

    AzkabanGobblinYarnAppLauncher launcher = buildLauncher(yarnLauncher, eventSubmitter);

    try {
      launcher.run();
    } catch (IOException ignored) {
    }

    ArgumentCaptor<GobblinEventBuilder> captor = ArgumentCaptor.forClass(GobblinEventBuilder.class);
    verify(eventSubmitter).submit(captor.capture());

    IssueEventBuilder issue = (IssueEventBuilder) captor.getValue();
    Assert.assertEquals(issue.getMetadata().get(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD), "testGroup");
    Assert.assertEquals(issue.getMetadata().get(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD), "testFlow");
    Assert.assertEquals(issue.getMetadata().get(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD), "12345");
    Assert.assertEquals(issue.getMetadata().get(TimingEvent.FlowEventConstants.JOB_NAME_FIELD), "testJob");
  }
}
