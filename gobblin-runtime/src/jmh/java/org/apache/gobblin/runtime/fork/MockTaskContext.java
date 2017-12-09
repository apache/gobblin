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

import com.google.common.collect.Lists;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicy;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyCheckResults;
import org.apache.gobblin.qualitychecker.row.RowLevelPolicyChecker;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.DataWriterBuilder;

import java.io.IOException;


class MockTaskContext extends TaskContext {
  public MockTaskContext(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public DataWriterBuilder getDataWriterBuilder(int branches, int index) {
    return new MockDataWriterBuilder();
  }

  @Override
  public RowLevelPolicyChecker getRowLevelPolicyChecker() throws Exception {
    return new MockRowLevelPolicyChecker();
  }

  @Override
  public RowLevelPolicyChecker getRowLevelPolicyChecker(int index) throws Exception {
    return new MockRowLevelPolicyChecker();
  }

  private class MockRowLevelPolicyChecker extends RowLevelPolicyChecker {
    MockRowLevelPolicyChecker() throws IOException {
      super(Lists.<RowLevelPolicy>newArrayList(), "0", null);
    }

    @Override
    public boolean executePolicies(Object record, RowLevelPolicyCheckResults results) throws IOException {
      return true;
    }
  }

  private static class MockDataWriterBuilder extends DataWriterBuilder {
    @Override
    public DataWriter build() throws IOException {
      return new MockDataWriterBuilder.MockDataWriter();
    }

    private static class MockDataWriter implements DataWriter {
      private int count = 0;

      @Override
      public void write(Object record) throws IOException {
        count++;
      }

      @Override
      public void commit() throws IOException {
      }

      @Override
      public void cleanup() throws IOException {
      }

      @Override
      public long recordsWritten() {
        return count;
      }

      @Override
      public long bytesWritten() throws IOException {
        return 0;
      }

      @Override
      public void close() throws IOException {
      }
    }
  }
}
