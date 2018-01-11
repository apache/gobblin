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

package org.apache.gobblin.metrics.reporter;

import java.io.IOException;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.FailureEventBuilder;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import avro.shaded.com.google.common.collect.Maps;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class FileFailureEventReporterTest {
  @Test
  public void testReport()
      throws IOException {
    MetricContext testContext = MetricContext.builder(getClass().getCanonicalName()).build();
    FileSystem fs = mock(FileSystem.class);
    Path failureLogPath = mock(Path.class);
    FSDataOutputStream outputStream = mock(FSDataOutputStream.class);

    FileFailureEventReporter reporter = new FileFailureEventReporter(testContext, fs, failureLogPath);
    when(fs.exists(any())).thenReturn(true);
    when(fs.append(any())).thenReturn(outputStream);

    final String eventName = "testEvent";
    final String eventNamespace = "testNamespace";
    GobblinTrackingEvent event =
        new GobblinTrackingEvent(0L, eventNamespace, eventName, Maps.newHashMap());

    // Noop on normal event
    testContext.submitEvent(event);
    verify(fs, never()).append(failureLogPath);
    verify(outputStream, never()).write(anyByte());

    // Process failure event
    FailureEventBuilder failureEvent = new FailureEventBuilder(eventName, eventNamespace);
    failureEvent.submit(testContext);
    reporter.report();
    // Failure log output is setup
    verify(fs, times(1)).append(failureLogPath);
    // Report successfully
    doAnswer( invocation -> null ).when(outputStream).write(any(byte[].class), anyInt(), anyInt());
    verify(outputStream, times(1)).write(any(byte[].class), anyInt(), anyInt());
  }
}
