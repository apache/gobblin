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

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.runtime.BoundedBlockingRecordQueue;
import org.apache.gobblin.runtime.ExecutionModel;
import org.apache.gobblin.runtime.TaskContext;
import org.apache.gobblin.util.concurrent.AutoResetEvent;

import java.io.IOException;


@SuppressWarnings("unchecked")
public class SynchronousFork extends Fork {
  private AutoResetEvent autoResetEvent;
  private volatile Throwable throwable;

  public SynchronousFork(TaskContext taskContext, Object schema, int branches, int index, ExecutionModel executionModel)
      throws Exception {
    super(taskContext, schema, branches, index, executionModel);
    this.autoResetEvent = new AutoResetEvent();
  }

  @Override
  protected void processRecords() throws IOException, DataConversionException {
    try {
      this.autoResetEvent.waitOne();
      if (this.throwable != null) {
        Throwables.propagateIfPossible(this.throwable, IOException.class, DataConversionException.class);
        throw new RuntimeException(throwable);
      }
    } catch (InterruptedException ie) {
      Throwables.propagate(ie);
    }
  }

  @Override
  protected boolean putRecordImpl(Object record) throws InterruptedException {
    try {
      this.processRecord(record);
    } catch (Throwable t) {
      this.throwable = t;
      this.autoResetEvent.set();
    }
    return true;
  }

  @Override
  public void markParentTaskDone() {
    super.markParentTaskDone();
    this.autoResetEvent.set();
  }

  @Override
  public Optional<BoundedBlockingRecordQueue<Object>.QueueStats> queueStats() {
    return Optional.absent();
  }
}
