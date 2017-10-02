package org.apache.gobblin.metrics.reporter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.notification.FailureEventNotification;
import org.apache.gobblin.metrics.notification.Notification;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class FileFailureEventReporter extends OutputStreamEventReporter {
  private final FileSystem fs;
  private final Path failureLogFile;
  private boolean hasSetupOutputStream;

  public FileFailureEventReporter(MetricContext context, FileSystem fs, Path failureLogFile)
      throws IOException {
    super(OutputStreamEventReporter.forContext(context));
    this.fs = fs;
    this.failureLogFile = failureLogFile;
    hasSetupOutputStream = false;
  }

  @Override
  public void notificationCallback(Notification notification) {
    if (notification instanceof FailureEventNotification) {
      setupOutputStream();
      addEventToReportingQueue(((FailureEventNotification) notification).getEvent());
    }
  }

  private void setupOutputStream() {
    if (hasSetupOutputStream) {
      // Already setup
      return;
    }

    try {
      boolean append = false;
      if (fs.exists(failureLogFile)) {
        log.info("Failure log file %s already exists, appending to it", failureLogFile);
        append = true;
      }
      OutputStream outputStream = append ? fs.append(failureLogFile) : fs.create(failureLogFile);
      output = this.closer.register(new PrintStream(outputStream, false, Charsets.UTF_8.toString()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      hasSetupOutputStream = true;
    }
  }
}
