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

package org.apache.gobblin.runtime.fork;

import java.io.IOException;
import lombok.Getter;
import lombok.Setter;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.runtime.ExecutionModel;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;
import org.apache.gobblin.writer.RetryWriter;
import org.junit.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ForkTest {
  @DataProvider(name = "closeConfigProvider")
  public static Object[][] closeConfigProvider() {
    // {close on done, expected count}
    return new Object[][]{{"true", 1}, {"false", 0}};
  }

  @Test(dataProvider = "closeConfigProvider")
  public void TestForCloseWriterTrue(String closeConfig, int expectedCloseCount) throws Exception {
    WorkUnitState wus = new WorkUnitState();
    wus.setProp(ConfigurationKeys.FORK_CLOSE_WRITER_ON_COMPLETION, closeConfig);
    wus.setProp(ConfigurationKeys.JOB_ID_KEY, "job2");
    wus.setProp(ConfigurationKeys.TASK_ID_KEY, "task1");
    wus.setProp(RetryWriter.RETRY_WRITER_ENABLED, "false");
    wus.setProp(ConfigurationKeys.WRITER_EAGER_INITIALIZATION_KEY, "true");
    wus.setProp(ConfigurationKeys.WRITER_BUILDER_CLASS, DummyDataWriterBuilder.class.getName());

    TaskContext taskContext = new TaskContext(wus);
    Fork testFork = new TestFork(taskContext, null, 0, 0, ExecutionModel.BATCH);

    Assert.assertNotNull(testFork.getWriter());

    testFork.run();

    Assert.assertEquals(expectedCloseCount, DummyDataWriterBuilder.getWriter().getCloseCount());
  }

  private static class TestFork extends Fork {

    public TestFork(TaskContext taskContext, Object schema, int branches, int index, ExecutionModel executionModel)
        throws Exception {
      super(taskContext, schema, branches, index, executionModel);
    }

    @Override
    protected void processRecords() throws IOException, DataConversionException {
    }

    @Override
    protected boolean putRecordImpl(Object record) throws InterruptedException {
      return true;
    }
  }

  public static class DummyDataWriterBuilder extends DataWriterBuilder<String, Integer> {
    private static ThreadLocal<DummyWriter> myThreadLocal = ThreadLocal.withInitial(() -> new DummyWriter());

    @Override
    public DataWriter<Integer> build() throws IOException {
      getWriter().setCloseCount(0);
      return getWriter();
    }

    public static DummyWriter getWriter() {
      return myThreadLocal.get();
    }
  }

  private static class DummyWriter implements DataWriter<Integer> {
    @Getter
    @Setter
    private int closeCount = 0;

    DummyWriter() {
    }

    @Override
    public void write(Integer record) throws IOException {
    }

    @Override
    public void commit() throws IOException {
    }

    @Override
    public void cleanup() throws IOException {
    }

    @Override
    public long recordsWritten() {
      return 0;
    }

    @Override
    public long bytesWritten() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {
      this.closeCount++;
    }
  }
}
