package org.apache.gobblin.metrics.reporter;

import java.io.IOException;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
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

    GobblinTrackingEvent event =
        new GobblinTrackingEvent(0L, "testNamespace", "TestEvent", Maps.newHashMap());

    // Noop on normal event
    testContext.submitEvent(event);
    verify(fs, never()).append(failureLogPath);
    verify(outputStream, never()).write(anyByte());

    // Process failure event
    testContext.submitFailureEvent(event);
    // Failure log output is setup
    verify(fs, times(1)).append(failureLogPath);
    // Report successfully
    doAnswer( invocation -> null ).when(outputStream).write(any(byte[].class), anyInt(), anyInt());
    reporter.report();
    verify(outputStream, times(1)).write(any(byte[].class), anyInt(), anyInt());
  }
}
