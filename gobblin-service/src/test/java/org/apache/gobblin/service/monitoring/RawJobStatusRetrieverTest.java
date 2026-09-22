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

package org.apache.gobblin.service.monitoring;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Predicate;

import org.testng.Assert;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.MysqlJobStatusStateStore;
import org.apache.gobblin.metastore.StateStore;
import org.apache.gobblin.metrics.RootMetricContext;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;


public class RawJobStatusRetrieverTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testGenericReaderUsesExactExecutionPrefixAndPreservesOpaqueMetadata() throws IOException {
    StateStore<State> store = mock(StateStore.class);
    JobStatusRetriever retriever = mock(JobStatusRetriever.class, CALLS_REAL_METHODS);
    doReturn(store).when(retriever).getStateStore();
    when(store.getTableNames(eq("group.flow"), any())).thenAnswer(invocation -> {
      Predicate<String> predicate = invocation.getArgument(1);
      return Arrays.asList("1234.group.job.gst", "1234.NA.NA.gst", "12340.group.other.gst")
          .stream().filter(predicate::apply).collect(Collectors.toList());
    });
    State job = new State();
    job.setProp("backend.handle", "opaque-original-handle");
    State summary = new State();
    when(store.getAll("group.flow", "1234.group.job.gst")).thenReturn(Collections.singletonList(job));
    when(store.getAll("group.flow", "1234.NA.NA.gst")).thenReturn(Collections.singletonList(summary));

    List<State> states = retriever.getJobStatusStatesForFlowExecution("flow", "group", 1234L);

    Assert.assertEquals(states, Arrays.asList(job, summary));
    Assert.assertEquals(states.get(0).getProp("backend.handle"), "opaque-original-handle");
    verify(store, never()).getAll("group.flow", "12340.group.other.gst");
    verify(store, never()).getAll("group.flow");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGenericReaderPropagatesReadFailureInsteadOfReturningNoRecords() throws IOException {
    StateStore<State> store = mock(StateStore.class);
    JobStatusRetriever retriever = mock(JobStatusRetriever.class, CALLS_REAL_METHODS);
    doReturn(store).when(retriever).getStateStore();
    IOException failure = new IOException("Unavailable");
    doThrow(failure).when(store).getTableNames(eq("group.flow"), any());

    Assert.assertSame(Assert.expectThrows(IOException.class,
        () -> retriever.getJobStatusStatesForFlowExecution("flow", "group", 1234L)), failure);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMysqlReaderReusesRawStoreAndPropagatesIOException() throws IOException {
    MysqlJobStatusStateStore<State> store = mock(MysqlJobStatusStateStore.class);
    MysqlJobStatusRetriever retriever = mock(MysqlJobStatusRetriever.class, CALLS_REAL_METHODS);
    setInternalState(retriever, "stateStore", store);
    setInternalState(retriever, "metricContext", RootMetricContext.get());
    State state = new State();
    state.setProp("backend.handle", "opaque");
    when(store.getAll("group.flow", 1234L)).thenReturn(Collections.singletonList(state));

    Assert.assertEquals(retriever.getJobStatusStatesForFlowExecution("flow", "group", 1234L),
        Collections.singletonList(state));
    verify(store).getAll("group.flow", 1234L);

    IOException failure = new IOException("Database unavailable");
    doThrow(failure).when(store).getAll("group.flow", 1234L);
    Assert.assertSame(Assert.expectThrows(IOException.class,
        () -> retriever.getJobStatusStatesForFlowExecution("flow", "group", 1234L)), failure);
  }
}
