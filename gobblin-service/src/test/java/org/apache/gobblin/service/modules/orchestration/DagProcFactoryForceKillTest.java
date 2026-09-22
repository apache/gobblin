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

package org.apache.gobblin.service.modules.orchestration;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.multibindings.OptionalBinder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.service.modules.orchestration.proc.KillDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.ReevaluateDagProc;
import org.apache.gobblin.service.modules.orchestration.proc.OrchestratorIssueEmitter;
import org.apache.gobblin.service.modules.orchestration.task.DagProcessingEngineMetrics;
import org.apache.gobblin.service.modules.orchestration.task.KillDagTask;
import org.apache.gobblin.service.modules.orchestration.task.ReevaluateDagTask;
import org.apache.gobblin.service.modules.utils.FlowCompilationValidationHelper;
import org.apache.gobblin.service.monitoring.JobStatusRetriever;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;


public class DagProcFactoryForceKillTest {
  @DataProvider
  public Object[][] handlerBindings() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "handlerBindings")
  public void testOptionalGuiceBindingIsLazyAndUsedOnlyForMissingDagKill(boolean bindHandler) throws IOException {
    AtomicInteger handlerRequests = new AtomicInteger();
    AtomicInteger retrieverRequests = new AtomicInteger();
    ForceKillHandler handler = mock(ForceKillHandler.class);
    JobStatusRetriever retriever = mock(JobStatusRetriever.class);
    Provider<ForceKillHandler> handlerProvider = () -> {
      handlerRequests.incrementAndGet();
      return handler;
    };
    Provider<JobStatusRetriever> retrieverProvider = () -> {
      retrieverRequests.incrementAndGet();
      return retriever;
    };
    Injector injector = Guice.createInjector(binder -> {
      binder.requireExplicitBindings();
      binder.bind(Config.class).toInstance(ConfigFactory.empty());
      binder.bind(FlowCompilationValidationHelper.class).toInstance(mock(FlowCompilationValidationHelper.class));
      binder.bind(JobStatusRetriever.class).toProvider(retrieverProvider);
      OptionalBinder<ForceKillHandler> optional = OptionalBinder.newOptionalBinder(binder, ForceKillHandler.class);
      if (bindHandler) {
        optional.setBinding().toProvider(handlerProvider);
      }
      binder.bind(DagProcFactory.class);
    });
    DagProcFactory factory = injector.getInstance(DagProcFactory.class);
    DagManagementStateStore store = mock(DagManagementStateStore.class);
    DagProcessingEngineMetrics metrics = mock(DagProcessingEngineMetrics.class);
    when(store.getDag(any())).thenReturn(Optional.empty());
    when(handler.forceKill(any(), anyList(), any())).thenReturn(ForceKillHandler.Result.ACKNOWLEDGED);
    State state = new State();
    state.setProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD, "group");
    state.setProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD, "flow");
    state.setProp(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD, "1234");
    state.setProp(TimingEvent.FlowEventConstants.JOB_GROUP_FIELD, "group");
    state.setProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD, "job");
    when(retriever.getJobStatusStatesForFlowExecution("flow", "group", 1234L))
        .thenReturn(Collections.singletonList(state));

    ReevaluateDagTask reevaluate = mock(ReevaluateDagTask.class);
    when(reevaluate.getDagAction()).thenReturn(action(DagActionStore.DagActionType.REEVALUATE));
    Assert.assertEquals(factory.meet(reevaluate).getClass(), ReevaluateDagProc.class);
    KillDagProc kill = factory.meet(new KillDagTask(action(DagActionStore.DagActionType.KILL), null, store, metrics));
    Assert.assertEquals(handlerRequests.get(), 0);
    Assert.assertEquals(retrieverRequests.get(), 0);

    try (MockedStatic<OrchestratorIssueEmitter> ignored = mockStatic(OrchestratorIssueEmitter.class)) {
      kill.process(store, metrics);
    }

    Assert.assertEquals(handlerRequests.get(), bindHandler ? 1 : 0);
    Assert.assertEquals(retrieverRequests.get(), bindHandler ? 1 : 0);
    verify(metrics).markDagActionsAct(DagActionStore.DagActionType.KILL, bindHandler);
    if (bindHandler) {
      verify(handler).forceKill(any(), anyList(), any());
    } else {
      verifyNoInteractions(handler, retriever);
    }
  }

  @Test
  public void testLegacyConstructorStillCreatesOrdinaryKillProcessor() throws IOException {
    DagProcFactory factory = new DagProcFactory(ConfigFactory.empty(), null);
    DagManagementStateStore store = mock(DagManagementStateStore.class);
    DagProcessingEngineMetrics metrics = mock(DagProcessingEngineMetrics.class);
    when(store.getDag(any())).thenReturn(Optional.empty());

    try (MockedStatic<OrchestratorIssueEmitter> ignored = mockStatic(OrchestratorIssueEmitter.class)) {
      factory.meet(new KillDagTask(action(DagActionStore.DagActionType.KILL), null, store, metrics))
          .process(store, metrics);
    }

    verify(metrics).markDagActionsAct(DagActionStore.DagActionType.KILL, false);
  }

  private static DagActionStore.DagAction action(DagActionStore.DagActionType type) {
    return DagActionStore.DagAction.forFlow("group", "flow", 1234L, type);
  }
}
