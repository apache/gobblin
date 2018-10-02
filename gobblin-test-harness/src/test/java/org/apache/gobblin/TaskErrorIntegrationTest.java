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
package org.apache.gobblin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.extractor.InstrumentedExtractor;
import org.apache.gobblin.runtime.GobblinMultiTaskAttempt;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;


@Test(enabled=false)
public class TaskErrorIntegrationTest {
  private static String EXCEPTION_MESSAGE = "test exception";

  @BeforeTest
  @AfterTest
  public void cleanDir()
      throws IOException {
    GobblinLocalJobLauncherUtils.cleanDir();
  }

  /**
   * Test that an extractor that raises an error on creation results in a log message from {@link GobblinMultiTaskAttempt}
   * @throws Exception
   */
  @Test
  public void extractorCreationError()
      throws Exception {
    TestAppender testAppender = new TestAppender();
    Logger logger = LogManager.getLogger(GobblinMultiTaskAttempt.class.getName() + "-noattempt");
    logger.addAppender(testAppender);

    Properties jobProperties =
        GobblinLocalJobLauncherUtils.getJobProperties("runtime_test/skip_workunits_test.properties");

    jobProperties.setProperty(ConfigurationKeys.SOURCE_CLASS_KEY, TestSource.class.getName());

    GobblinLocalJobLauncherUtils.invokeLocalJobLauncher(jobProperties);

    Assert.assertTrue(testAppender.events.stream().anyMatch(e -> e.getRenderedMessage()
        .startsWith("Could not create task for workunit")));
  }

  /**
   * Test extractor that raises an exception on construction
   */
  public static class TestExtractor<S, D> extends InstrumentedExtractor<S, D> {

    public TestExtractor(WorkUnitState workUnitState) {
      super(workUnitState);

      throw new RuntimeException(EXCEPTION_MESSAGE);
    }

    @Override
    public S getSchema() throws IOException {
      return null;
    }

    @Override
    public long getExpectedRecordCount() {
      return 0;
    }

    @Override
    public long getHighWatermark() {
      return 0;
    }

    @Override
    public D readRecordImpl(D reuse) throws DataRecordException, IOException {
      return null;
    }
  }

  /**
   * Test source that creates a {@link TestExtractor}
   */
  public static class TestSource implements Source<Schema, GenericRecord> {

    @Override
    public List<WorkUnit> getWorkunits(SourceState state) {
      WorkUnit workUnit = WorkUnit.createEmpty();
      workUnit.addAll(state);
      return Collections.singletonList(workUnit);
    }

    @Override
    public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
        throws IOException {
      return new TestExtractor(state);
    }

    @Override
    public void shutdown(SourceState state) {
    }
  }

  private class TestAppender extends AppenderSkeleton {
    List<LoggingEvent> events = new ArrayList<LoggingEvent>();
    public void close() {}
    public boolean requiresLayout() {return false;}
    @Override
    protected void append(LoggingEvent event) {
      events.add(event);
    }
  }
}
