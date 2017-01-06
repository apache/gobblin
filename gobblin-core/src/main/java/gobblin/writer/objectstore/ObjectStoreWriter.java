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
package gobblin.writer.objectstore;

import java.io.IOException;

import lombok.Getter;

import com.codahale.metrics.Counter;

import gobblin.annotation.Alpha;
import gobblin.configuration.State;
import gobblin.instrumented.writer.InstrumentedDataWriter;

/**
 * A writer to execute operations on a object in any object store. The record type of this writer is an {@link ObjectStoreOperation}.
 * The {@link ObjectStoreOperation} encapsulates operation specific metadata and actions.
 */
@Alpha
@SuppressWarnings("rawtypes")
public class ObjectStoreWriter extends InstrumentedDataWriter<ObjectStoreOperation> {

  private static final String OPERATIONS_EXECUTED_COUNTER = "gobblin.objectStoreWriter.operationsExecuted";
  private final Counter operationsExecuted;
  @Getter
  private final ObjectStoreClient objectStoreClient;

  public ObjectStoreWriter(ObjectStoreClient client, State state) {
    super(state);
    this.objectStoreClient = client;
    this.operationsExecuted = this.getMetricContext().counter(OPERATIONS_EXECUTED_COUNTER);
  }

  @Override
  public void close() throws IOException {
    this.objectStoreClient.close();
  }

  /**
   * Calls {@link ObjectStoreOperation#execute(ObjectStoreClient)} on the <code>operation</code> passed
   *
   * {@inheritDoc}
   * @see gobblin.writer.DataWriter#write(java.lang.Object)
   */
  @Override
  public void writeImpl(ObjectStoreOperation operation) throws IOException {
    operation.execute(this.getObjectStoreClient());
    this.operationsExecuted.inc();
  }

  @Override
  public void commit() throws IOException {
  }

  @Override
  public void cleanup() throws IOException {
    this.getObjectStoreClient().close();
  }

  @Override
  public long recordsWritten() {
    return this.operationsExecuted.getCount();
  }

  @Override
  public long bytesWritten() throws IOException {
    // TODO Will be added when ObjectStorePutOperation is implemented. Currently we only support ObjectStoreDeleteOperation
    return 0;
  }
}
