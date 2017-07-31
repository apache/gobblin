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

package org.apache.gobblin.writer.test;

import java.io.IOException;
import java.util.Queue;

import org.apache.avro.Schema;

import com.google.common.collect.Queues;

import org.apache.gobblin.commit.SpeculativeAttemptAwareConstruct;
import org.apache.gobblin.writer.DataWriter;
import org.apache.gobblin.writer.PartitionAwareDataWriterBuilder;

import lombok.Data;


public class TestPartitionAwareWriterBuilder extends PartitionAwareDataWriterBuilder<String, String> {

  public final Queue<Action> actions = Queues.newArrayDeque();

  public enum Actions {
    BUILD, WRITE, COMMIT, CLEANUP, CLOSE
  }

  @Override
  public boolean validatePartitionSchema(Schema partitionSchema) {
    return true;
  }

  @Override
  public DataWriter build()
      throws IOException {
    String partition = this.partition.get().get(TestPartitioner.PARTITION).toString();
    this.actions.add(new Action(Actions.BUILD, partition, null));
    if (partition.matches(".*\\d+.*")) {
      return new SpeculativeNotSafeTestWriter(partition);
    }
    return new TestDataWriter(partition);
  }

  private class SpeculativeNotSafeTestWriter extends TestDataWriter {

    public SpeculativeNotSafeTestWriter(String partition) {
      super(partition);
    }

    @Override
    public boolean isSpeculativeAttemptSafe() {
      return false;
    }
  }

  private class TestDataWriter implements DataWriter<String>, SpeculativeAttemptAwareConstruct {

    private String partition;
    private long recordsWritten = 0;
    private long bytesWritten = 0;

    public TestDataWriter(String partition) {
      this.partition = partition;
    }

    @Override
    public void write(String record)
        throws IOException {
      actions.add(new Action(Actions.WRITE, this.partition, record));
      this.recordsWritten++;
      this.bytesWritten++;
    }

    @Override
    public void commit()
        throws IOException {
      actions.add(new Action(Actions.COMMIT, this.partition, null));
    }

    @Override
    public void cleanup()
        throws IOException {
      actions.add(new Action(Actions.CLEANUP, this.partition, null));
    }

    @Override
    public long recordsWritten() {
      return this.recordsWritten;
    }

    @Override
    public long bytesWritten()
        throws IOException {
      return this.bytesWritten;
    }

    @Override
    public void close()
        throws IOException {
      actions.add(new Action(Actions.CLOSE, this.partition, null));
    }

    @Override
    public boolean isSpeculativeAttemptSafe() {
      return true;
    }
  }

  @Data
  public static class Action {
    private final Actions type;
    private final String partition;
    private final String target;
  }
}
