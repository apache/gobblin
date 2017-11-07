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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Queue;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.FailureEventBuilder;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;

import lombok.extern.slf4j.Slf4j;


/**
 * An {@link OutputStreamEventReporter} reports only failure event build by {@link FailureEventBuilder}. It won't create
 * the failure log file until a failure event is processed
 */
@Slf4j
public class FileFailureEventReporter extends OutputStreamEventReporter {
  private final FileSystem fs;
  private final Path failureLogFile;
  private volatile boolean hasSetupOutputStream;

  public FileFailureEventReporter(MetricContext context, FileSystem fs, Path failureLogFile)
      throws IOException {
    super(OutputStreamEventReporter.forContext(context));
    this.fs = fs;
    this.failureLogFile = failureLogFile;
    hasSetupOutputStream = false;
  }

  @Override
  public void addEventToReportingQueue(GobblinTrackingEvent event) {
    if (FailureEventBuilder.isFailureEvent(event)) {
      super.addEventToReportingQueue(event);
    }
  }

  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    if (queue.size() > 0) {
      setupOutputStream();
      super.reportEventQueue(queue);
    }
  }

  /**
   * Set up the {@link OutputStream} to the {@link #failureLogFile} only once
   */
  private void setupOutputStream() {
    synchronized (failureLogFile) {
      // Setup is done by some thread
      if (hasSetupOutputStream) {
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
}
