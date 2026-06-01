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

package org.apache.gobblin.yarn;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.apache.gobblin.metrics.event.TimingEvent;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;


/**
 * Focused unit tests for the launcher-side exit-code + terminal-hook dispatch logic in
 * {@link GobblinYarnAppLauncher}. The OSS class itself does not emit GTEs (a subclass overrides the hooks to do
 * that), so these tests cover only: the {@link FinalApplicationStatus} -> event-name mapping, the exit-code
 * propagation, and that the protected hooks are dispatched. Avoids the full {@link MiniYARNCluster} / Helix
 * stack by constructing a real-methods Mockito stand-in and injecting the {@code exitCode} field the code reads.
 */
public class GobblinYarnAppLauncherTerminalGteTest {

  private GobblinYarnAppLauncher launcher;
  private ApplicationId applicationId;

  @BeforeMethod
  public void setUp() throws Exception {
    this.launcher = mock(GobblinYarnAppLauncher.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    this.applicationId = mock(ApplicationId.class);
    when(this.applicationId.toString()).thenReturn("application_1700000000000_0001");
    setField("exitCode", 0);
    setField("terminalHandled", new AtomicBoolean(false));
  }

  private void setField(String name, Object value) throws Exception {
    Field f = GobblinYarnAppLauncher.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(this.launcher, value);
  }

  private int readExitCode() throws Exception {
    Field f = GobblinYarnAppLauncher.class.getDeclaredField("exitCode");
    f.setAccessible(true);
    return (int) f.get(this.launcher);
  }

  private ApplicationReport mockReport(FinalApplicationStatus status) {
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getApplicationId()).thenReturn(this.applicationId);
    when(report.getFinalApplicationStatus()).thenReturn(status);
    return report;
  }

  // ---------- mapFinalAppStatusToEventName ----------

  @Test
  public void testMapFinalAppStatusSucceeded() {
    assertEquals(GobblinYarnAppLauncher.mapFinalAppStatusToEventName(FinalApplicationStatus.SUCCEEDED),
        TimingEvent.LauncherTimings.JOB_SUCCEEDED);
  }

  @Test
  public void testMapFinalAppStatusKilled() {
    assertEquals(GobblinYarnAppLauncher.mapFinalAppStatusToEventName(FinalApplicationStatus.KILLED),
        TimingEvent.LauncherTimings.JOB_CANCEL);
  }

  @Test
  public void testMapFinalAppStatusFailed() {
    assertEquals(GobblinYarnAppLauncher.mapFinalAppStatusToEventName(FinalApplicationStatus.FAILED),
        TimingEvent.LauncherTimings.JOB_FAILED);
  }

  @Test
  public void testMapFinalAppStatusUndefined() {
    assertEquals(GobblinYarnAppLauncher.mapFinalAppStatusToEventName(FinalApplicationStatus.UNDEFINED),
        TimingEvent.LauncherTimings.JOB_FAILED);
  }

  // ---------- handleTerminalAppStatus: exit-code wiring + hook dispatch ----------

  @Test
  public void testHandleTerminalAppStatusFailedSetsExitCodeOne() throws Exception {
    this.launcher.handleTerminalAppStatus(mockReport(FinalApplicationStatus.FAILED));
    assertEquals(readExitCode(), 1);
  }

  @Test
  public void testHandleTerminalAppStatusKilledSetsExitCodeOne() throws Exception {
    this.launcher.handleTerminalAppStatus(mockReport(FinalApplicationStatus.KILLED));
    assertEquals(readExitCode(), 1);
  }

  @Test
  public void testHandleTerminalAppStatusUndefinedSetsExitCodeOne() throws Exception {
    this.launcher.handleTerminalAppStatus(mockReport(FinalApplicationStatus.UNDEFINED));
    assertEquals(readExitCode(), 1);
  }

  @Test
  public void testHandleTerminalAppStatusSucceededLeavesExitCodeZero() throws Exception {
    this.launcher.handleTerminalAppStatus(mockReport(FinalApplicationStatus.SUCCEEDED));
    assertEquals(readExitCode(), 0);
  }

  @Test
  public void testHandleTerminalAppStatusDispatchesToHook() {
    // The protected hook is the seam a subclass (e.g. RobinGobblinYarnAppLauncher) overrides to emit the single
    // terminal GTE; verify it is invoked with the observed report + status.
    ApplicationReport report = mockReport(FinalApplicationStatus.FAILED);
    this.launcher.handleTerminalAppStatus(report);
    Mockito.verify(this.launcher).onTerminalApplicationStatus(report, FinalApplicationStatus.FAILED);
  }

  // ---------- handleLostAmVisibility ----------

  @Test
  public void testHandleLostAmVisibilitySetsExitCodeAndDispatchesHook() throws Exception {
    this.launcher.handleLostAmVisibility();
    assertEquals(readExitCode(), 1);
    Mockito.verify(this.launcher).onLostAmVisibility();
  }

  // ---------- handleApplicationLaunchFailure ----------

  @Test
  public void testHandleApplicationLaunchFailureSetsExitCodeAndDispatchesHook() throws Exception {
    RuntimeException cause = new RuntimeException("submit failed");
    this.launcher.handleApplicationLaunchFailure(cause);
    assertEquals(readExitCode(), 1);
    Mockito.verify(this.launcher).onApplicationLaunchFailure(cause);
  }

  @Test
  public void testTerminalHandledGuardDispatchesOnlyOnce() throws Exception {
    // The first terminal outcome wins; a later path (e.g. the cancel/kill path firing after a SUCCEEDED was
    // already dispatched by the monitor) must be a no-op and must not override the exit code or re-dispatch.
    this.launcher.handleTerminalAppStatus(mockReport(FinalApplicationStatus.SUCCEEDED));
    this.launcher.handleTerminalAppStatus(mockReport(FinalApplicationStatus.FAILED));
    assertEquals(readExitCode(), 0);
    Mockito.verify(this.launcher, Mockito.times(1))
        .onTerminalApplicationStatus(Mockito.any(), Mockito.any());
  }
}
