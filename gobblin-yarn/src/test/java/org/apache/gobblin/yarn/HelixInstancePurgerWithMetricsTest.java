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

import com.google.common.base.Stopwatch;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


/**
 * Test class uses PowerMockito and Testng
 * References:
 * https://github.com/powermock/powermock/issues/434
 * https://www.igorkromin.net/index.php/2018/10/04/how-to-fix-powermock-exception-linkageerror-loader-constraint-violation/
 * https://github.com/powermock/powermock/wiki/MockFinal
 */
@PrepareForTest(Stopwatch.class)
@PowerMockIgnore("javax.management.*")
public class HelixInstancePurgerWithMetricsTest extends PowerMockTestCase {

  @Mock EventSubmitter eventSubmitter;
  @Mock Stopwatch stopwatch;
  @Mock CompletableFuture<Void> mockTask;
  @Captor ArgumentCaptor<GobblinEventBuilder> gteCaptor;
  HelixInstancePurgerWithMetrics sut;

  private static final long LAGGING_PURGE_THRESHOLD_MS = 100;
  private static final long PURGE_STATUS_POLLING_RATE_MS = 10;


  @BeforeMethod
  private void init() {
    MockitoAnnotations.initMocks(this);
    sut = new HelixInstancePurgerWithMetrics(eventSubmitter, PURGE_STATUS_POLLING_RATE_MS);
  }

  @Test
  public void testPurgeOfflineInstances() throws ExecutionException, InterruptedException {
    Mockito.when(stopwatch.start()).thenReturn(stopwatch);
    Mockito.when(stopwatch.elapsed(TimeUnit.MILLISECONDS)).thenReturn(LAGGING_PURGE_THRESHOLD_MS);
    Mockito.when(mockTask.isDone())
        .thenReturn(false)
        .thenReturn(true);
    Mockito.when(mockTask.get()).thenReturn(null);
    Mockito.doNothing().when(eventSubmitter).submit(Mockito.any(GobblinEventBuilder.class));

    long elapsedTime = sut.waitForPurgeCompletion(mockTask, LAGGING_PURGE_THRESHOLD_MS, stopwatch, Collections.emptyMap());

    assertEquals(elapsedTime, LAGGING_PURGE_THRESHOLD_MS);
    Mockito.verify(stopwatch, times(1)).start();
    Mockito.verify(mockTask, times(1)).get();
    Mockito.verify(eventSubmitter, times(1)).submit(gteCaptor.capture());
    assertEquals(gteCaptor.getValue().getName(), HelixInstancePurgerWithMetrics.PURGE_COMPLETED_EVENT);
  }

  @Test
  public void testPurgeOfflineInstancesSendsWarningEventWhenWaiting() throws ExecutionException, InterruptedException {
    Mockito.when(mockTask.isDone()).thenReturn(false).thenReturn(true);
    testPurgeOfflineInstancesSendsWarningEventHelper();
  }

  @Test
  public void testPurgeOfflineInstancesSendsWarningEventIfTaskFinishedImmediately() throws ExecutionException, InterruptedException {
    Mockito.when(mockTask.isDone()).thenReturn(true);
    testPurgeOfflineInstancesSendsWarningEventHelper();
  }

  private void testPurgeOfflineInstancesSendsWarningEventHelper() throws ExecutionException, InterruptedException {
    Mockito.when(stopwatch.start()).thenReturn(stopwatch);
    Mockito.when(stopwatch.elapsed(TimeUnit.MILLISECONDS)).thenReturn(LAGGING_PURGE_THRESHOLD_MS + 1);
    Mockito.when(mockTask.isDone()).thenReturn(false).thenReturn(true);
    Mockito.when(mockTask.get()).thenReturn(null);
    Mockito.doNothing().when(eventSubmitter).submit(Mockito.any(GobblinEventBuilder.class));

    long elapsedTime = sut.waitForPurgeCompletion(mockTask, LAGGING_PURGE_THRESHOLD_MS, stopwatch, Collections.emptyMap());
    assertEquals(elapsedTime, LAGGING_PURGE_THRESHOLD_MS + 1);

    Mockito.verify(stopwatch, times(1)).start();
    Mockito.verify(mockTask, times(1)).get();
    Mockito.verify(eventSubmitter, times(2)).submit(gteCaptor.capture());
    assertEquals(gteCaptor.getAllValues().get(0).getName(), HelixInstancePurgerWithMetrics.PURGE_LAGGING_EVENT);
    assertEquals(gteCaptor.getAllValues().get(1).getName(), HelixInstancePurgerWithMetrics.PURGE_COMPLETED_EVENT);
  }

  @Test
  public void testPurgeOfflineInstancesSendsFailureEvent() throws ExecutionException, InterruptedException {
    Mockito.when(stopwatch.start()).thenReturn(stopwatch);
    Mockito.when(stopwatch.elapsed(TimeUnit.MILLISECONDS)).thenReturn(LAGGING_PURGE_THRESHOLD_MS + 1);
    Mockito.when(mockTask.isDone()).thenReturn(true);
    Mockito.when(mockTask.get()).thenThrow(new ExecutionException("Throwing exception to emulate helix failure", new RuntimeException()));
    Mockito.doNothing().when(eventSubmitter).submit(Mockito.any(GobblinEventBuilder.class));

    long elapsedTime = sut.waitForPurgeCompletion(mockTask, LAGGING_PURGE_THRESHOLD_MS, stopwatch, Collections.emptyMap());
    assertEquals(elapsedTime, LAGGING_PURGE_THRESHOLD_MS +1);

    Mockito.verify(stopwatch, times(1)).start();
    Mockito.verify(mockTask, times(1)).get();
    Mockito.verify(eventSubmitter, times(2)).submit(gteCaptor.capture());
    assertEquals(gteCaptor.getAllValues().get(0).getName(), HelixInstancePurgerWithMetrics.PURGE_LAGGING_EVENT);
    assertEquals(gteCaptor.getAllValues().get(1).getName(), HelixInstancePurgerWithMetrics.PURGE_FAILURE_EVENT);
  }
}
