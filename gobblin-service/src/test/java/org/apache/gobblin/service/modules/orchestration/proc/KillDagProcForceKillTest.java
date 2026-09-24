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

package org.apache.gobblin.service.modules.orchestration.proc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.inject.Provider;
import com.typesafe.config.ConfigFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.ExecutionStatus;
import org.apache.gobblin.service.modules.flowgraph.Dag;
import org.apache.gobblin.service.modules.orchestration.DagActionStore;
import org.apache.gobblin.service.modules.orchestration.DagManagementStateStore;
import org.apache.gobblin.service.modules.orchestration.ForceKillHandler;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.spec.JobExecutionPlan;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


public class KillDagProcForceKillTest {
  private static final String GROUP = "group";
  private static final String FLOW = "flow";
  private static final long EXECUTION_ID = 1234L;

  private DagManagementStateStore store;
  private JobStatusRetriever retriever;
  private ForceKillHandler handler;
  private Provider<JobStatusRetriever> retrieverProvider;
  private Provider<ForceKillHandler> handlerProvider;
  private DagProcessingEngineMetrics metrics;
  private MockedStatic<DagProcUtils> dagUtils;
  private MockedStatic<OrchestratorIssueEmitter> issues;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp() throws IOException {
    this.store = mock(DagManagementStateStore.class);
    this.retriever = mock(JobStatusRetriever.class);
    this.handler = mock(ForceKillHandler.class);
    this.retrieverProvider = mock(Provider.class);
    this.handlerProvider = mock(Provider.class);
    this.metrics = mock(DagProcessingEngineMetrics.class);
    when(this.store.getDag(any())).thenReturn(Optional.empty());
    when(this.retrieverProvider.get()).thenReturn(this.retriever);
    when(this.handlerProvider.get()).thenReturn(this.handler);
    when(this.handler.forceKill(any(), anyList(), any())).thenReturn(ForceKillHandler.Result.ACKNOWLEDGED);
    statuses();
    this.dagUtils = mockStatic(DagProcUtils.class);
    this.issues = mockStatic(OrchestratorIssueEmitter.class);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() {
    if (this.issues != null) {
      this.issues.close();
    }
    if (this.dagUtils != null) {
      this.dagUtils.close();
    }
  }

  @DataProvider
  public Object[][] activeScopes() {
    return new Object[][] {{DagActionStore.NO_JOB_NAME_DEFAULT}, {"job"}};
  }

  @Test(dataProvider = "activeScopes")
  @SuppressWarnings("unchecked")
  public void testActiveDagUsesOnlyNormalCancellation(String jobName) throws IOException {
    Dag<JobExecutionPlan> dag = mock(Dag.class);
    Dag.DagNode<JobExecutionPlan> node = mock(Dag.DagNode.class);
    when(this.store.getDag(any())).thenReturn(Optional.of(dag));
    when(this.store.getDagNodeWithJobStatus(any())).thenReturn(Pair.of(Optional.of(node), Optional.empty()));

    processor(jobName).process(this.store, this.metrics);

    this.dagUtils.verify(() -> DagProcUtils.setAndEmitFlowEvent(any(), eq(dag),
        eq(TimingEvent.FlowTimings.FLOW_CANCELLED)));
    if (jobName.equals(DagActionStore.NO_JOB_NAME_DEFAULT)) {
      this.dagUtils.verify(() -> DagProcUtils.cancelDag(dag, this.store));
    } else {
      this.dagUtils.verify(() -> DagProcUtils.cancelDagNode(node, this.store));
    }
    verifyNoInteractions(this.handlerProvider, this.retrieverProvider, this.handler, this.retriever);
    verify(this.metrics).markDagActionsAct(DagActionStore.DagActionType.KILL, true);
    verify(this.store, never()).getFailedDag(any());
  }

  @Test
  public void testAbsentHandlerPreservesMissingDagBehavior() throws IOException {
    new KillDagProc(task(DagActionStore.NO_JOB_NAME_DEFAULT), ConfigFactory.empty()).process(this.store, this.metrics);

    verifyNoInteractions(this.handlerProvider, this.retrieverProvider, this.handler, this.retriever);
    assertFailedWithoutCancellation();
    verify(this.store, never()).getFailedDag(any());
  }

  @Test
  public void testAcknowledgedFallbackGetsImmutableDefensiveCopiesAndOriginalMetadata() throws IOException {
    State original = state(GROUP, "job");
    original.getCommonProperties().setProperty("backend.common.handle", "original-common-handle");
    original.getCommonProperties().setProperty("backend.target", "original-target");
    original.getCommonProperties().setProperty("backend.handle", "overridden-common-handle");
    original.setProp("backend.handle", "{\"generation\":1,\"attempt\":1,\"id\":\"original\"}");
    original.setProp(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD, "3");
    original.setProp(TimingEvent.FlowEventConstants.CURRENT_ATTEMPTS_FIELD, "4");
    statuses(original);
    doAnswer(invocation -> {
      List<State> states = invocation.getArgument(1);
      Assert.assertEquals(states.size(), 1);
      Assert.assertNotSame(states.get(0), original);
      Assert.assertEquals(states.get(0).getProp("backend.handle"), original.getProp("backend.handle"));
      Assert.assertEquals(states.get(0).getProp("backend.common.handle"), "original-common-handle");
      Assert.assertEquals(states.get(0).getProp(TimingEvent.FlowEventConstants.CURRENT_GENERATION_FIELD), "3");
      Assert.expectThrows(UnsupportedOperationException.class, () -> states.add(new State()));
      states.get(0).setProp("backend.handle", "changed by handler");
      states.get(0).getCommonProperties().setProperty("backend.common.handle", "changed by handler");
      original.getCommonProperties().setProperty("backend.target", "changed in source");
      Assert.assertEquals(states.get(0).getProp("backend.target"), "original-target");
      ForceKillHandler.ActiveDagCheck active = invocation.getArgument(2);
      Assert.assertFalse(active.isActive());
      return ForceKillHandler.Result.ACKNOWLEDGED;
    }).when(this.handler).forceKill(any(), anyList(), any());

    processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics);

    Assert.assertEquals(original.getProp("backend.handle"), "{\"generation\":1,\"attempt\":1,\"id\":\"original\"}");
    Assert.assertEquals(original.getProp("backend.common.handle"), "original-common-handle");
    verify(this.metrics).markDagActionsAct(DagActionStore.DagActionType.KILL, true);
    this.dagUtils.verifyNoInteractions();
    this.issues.verifyNoInteractions();
    verify(this.store, never()).getFailedDag(any());
    verify(this.store, never()).updateDagNode(any());
    verify(this.store, never()).deleteDag(any());
  }

  @DataProvider
  public Object[][] absentOrOutOfScopeStates() {
    return new Object[][] {
        {Collections.emptyList()},
        {Collections.singletonList(state(JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY))},
        {Collections.singletonList(withFlowField(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "12340"))},
        {Collections.singletonList(withFlowField(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "other"))},
        {Collections.singletonList(withFlowField(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "other"))}
    };
  }

  @Test(dataProvider = "absentOrOutOfScopeStates")
  public void testMissingOrOutOfScopeStateCannotTriggerHandler(List<State> states) throws IOException {
    when(this.retriever.getJobStatusStatesForFlowExecution(FLOW, GROUP, EXECUTION_ID)).thenReturn(states);

    processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics);

    verifyNoInteractions(this.handlerProvider, this.handler);
    assertFailedWithoutCancellation();
    verify(this.store, never()).getFailedDag(any());
  }

  @Test
  public void testJobSpecificScopeKeepsOnlyTargetAndSummary() throws IOException {
    State summary = state(JobStatusRetriever.NA_KEY, JobStatusRetriever.NA_KEY);
    statuses(summary, state(GROUP, "job"), state(GROUP, "other-job"), state("other-group", "job"));
    doAnswer(invocation -> {
      List<State> states = invocation.getArgument(1);
      Assert.assertEquals(states.size(), 2);
      Assert.assertEquals(states.get(0).getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD), JobStatusRetriever.NA_KEY);
      Assert.assertEquals(states.get(1).getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD), "job");
      Assert.assertEquals(states.get(1).getProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD), GROUP);
      return ForceKillHandler.Result.ACKNOWLEDGED;
    }).when(this.handler).forceKill(eq(action("job")), anyList(), any());

    processor("job").process(this.store, this.metrics);

    verify(this.handler).forceKill(eq(action("job")), anyList(), any());
    this.dagUtils.verifyNoInteractions();
  }

  @Test
  public void testUnacknowledgedHandlerDoesNotReportSuccess() throws IOException {
    statuses(state(GROUP, "job"));
    when(this.handler.forceKill(any(), anyList(), any())).thenReturn(ForceKillHandler.Result.NOT_ACKNOWLEDGED);

    processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics);

    assertFailedWithoutCancellation();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDagReappearingAfterStatusReadUsesNormalCancellation() throws IOException {
    Dag<JobExecutionPlan> dag = mock(Dag.class);
    statuses(state(GROUP, "job"));
    when(this.store.getDag(any())).thenReturn(Optional.empty(), Optional.of(dag));

    processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics);

    verifyNoInteractions(this.handlerProvider, this.handler);
    this.dagUtils.verify(() -> DagProcUtils.cancelDag(dag, this.store));
    verify(this.metrics).markDagActionsAct(DagActionStore.DagActionType.KILL, true);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDagReappearingDuringHandlerUsesNormalCancellation() throws IOException {
    Dag<JobExecutionPlan> dag = mock(Dag.class);
    statuses(state(GROUP, "job"));
    when(this.store.getDag(any())).thenReturn(Optional.empty(), Optional.empty(), Optional.of(dag));
    doAnswer(invocation -> {
      ForceKillHandler.ActiveDagCheck active = invocation.getArgument(2);
      Assert.assertTrue(active.isActive());
      return ForceKillHandler.Result.ACTIVE_DAG_PRESENT;
    }).when(this.handler).forceKill(any(), anyList(), any());

    processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics);

    this.dagUtils.verify(() -> DagProcUtils.cancelDag(dag, this.store));
    verify(this.metrics).markDagActionsAct(DagActionStore.DagActionType.KILL, true);
  }

  @Test
  public void testReappearedDagDisappearingAgainDoesNotResumeRetainedStopping() throws IOException {
    statuses(state(GROUP, "job"));
    when(this.handler.forceKill(any(), anyList(), any())).thenReturn(ForceKillHandler.Result.ACTIVE_DAG_PRESENT);

    processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics);

    assertFailedWithoutCancellation();
    verify(this.handler).forceKill(any(), anyList(), any());
  }

  @Test
  public void testReadFailurePropagatesWithoutCallingHandler() throws IOException {
    IOException failure = new IOException("Status read failed");
    doThrow(failure).when(this.retriever).getJobStatusStatesForFlowExecution(FLOW, GROUP, EXECUTION_ID);

    Assert.assertSame(Assert.expectThrows(IOException.class,
        () -> processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics)), failure);

    verifyNoInteractions(this.handlerProvider, this.handler);
    assertFailedWithoutCancellation();
  }

  @Test
  public void testHandlerFailurePropagatesWithoutSuccess() throws IOException {
    statuses(state(GROUP, "job"));
    IOException failure = new IOException("Backend unavailable");
    doThrow(failure).when(this.handler).forceKill(any(), anyList(), any());

    Assert.assertSame(Assert.expectThrows(IOException.class,
        () -> processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics)), failure);

    assertFailedWithoutCancellation();
  }

  @Test
  public void testActiveDagCheckFailurePropagates() throws IOException {
    statuses(state(GROUP, "job"));
    IOException failure = new IOException("Active DAG read failed");
    when(this.store.getDag(any())).thenReturn(Optional.empty(), Optional.empty()).thenThrow(failure);
    doAnswer(invocation -> {
      ForceKillHandler.ActiveDagCheck active = invocation.getArgument(2);
      active.isActive();
      return ForceKillHandler.Result.ACKNOWLEDGED;
    }).when(this.handler).forceKill(any(), anyList(), any());

    Assert.assertSame(Assert.expectThrows(IOException.class,
        () -> processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics)), failure);

    assertFailedWithoutCancellation();
  }

  @Test
  public void testNullHandlerResultIsNotSuccess() throws IOException {
    statuses(state(GROUP, "job"));
    when(this.handler.forceKill(any(), anyList(), any())).thenReturn(null);

    Assert.expectThrows(NullPointerException.class,
        () -> processor(DagActionStore.NO_JOB_NAME_DEFAULT).process(this.store, this.metrics));

    assertFailedWithoutCancellation();
  }

  private void assertFailedWithoutCancellation() {
    verify(this.metrics).markDagActionsAct(DagActionStore.DagActionType.KILL, false);
    verify(this.metrics, never()).markDagActionsAct(DagActionStore.DagActionType.KILL, true);
    this.dagUtils.verifyNoInteractions();
  }

  private void statuses(State... states) throws IOException {
    when(this.retriever.getJobStatusStatesForFlowExecution(FLOW, GROUP, EXECUTION_ID)).thenReturn(Arrays.asList(states));
  }

  private KillDagProc processor(String jobName) {
    return new KillDagProc(task(jobName), ConfigFactory.empty(), this.handlerProvider, this.retrieverProvider);
  }

  private KillDagTask task(String jobName) {
    return new KillDagTask(action(jobName), null, this.store, this.metrics);
  }

  private static DagActionStore.DagAction action(String jobName) {
    return new DagActionStore.DagAction(GROUP, FLOW, EXECUTION_ID, jobName, DagActionStore.DagActionType.KILL);
  }

  private static State withFlowField(String field, String value) {
    State state = state(GROUP, "job");
    state.setProp(field, value);
    return state;
  }

  private static State state(String jobGroup, String jobName) {
    State state = new State();
    state.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, GROUP);
    state.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, FLOW);
    state.setProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, Long.toString(EXECUTION_ID));
    state.setProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, jobGroup);
    state.setProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, jobName);
    state.setProp(JobStatusRetriever.EVENT_NAME_FIELD, ExecutionStatus.COMPLETE.name());
    return state;
  }
}
