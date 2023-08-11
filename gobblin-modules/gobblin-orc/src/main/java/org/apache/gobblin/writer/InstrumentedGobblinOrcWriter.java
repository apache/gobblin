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

package org.apache.gobblin.writer;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;


/***
 * A class for an event emitting GobblinOrcWriter metrics, such as internal memory resizing and flushing
 */
@Slf4j
public class InstrumentedGobblinOrcWriter extends GobblinOrcWriter {
  MetricContext metricContext;
  public static final String METRICS_SCHEMA_NAME = "schemaName";
  public static final String METRICS_BYTES_WRITTEN = "bytesWritten";
  public static final String METRICS_RECORDS_WRITTEN = "recordsWritten";
  public static final String METRICS_BUFFER_RESIZES = "bufferResizes";
  public static final String METRICS_BUFFER_SIZE = "bufferSize";
  public static final String ORC_WRITER_METRICS_NAME = "OrcWriterMetrics";
  private static final String ORC_WRITER_NAMESPACE = "gobblin.orc.writer";

  public InstrumentedGobblinOrcWriter(FsDataWriterBuilder<Schema, GenericRecord> builder, State properties) throws IOException {
    super(builder, properties);
    metricContext = Instrumented.getMetricContext(new State(properties), this.getClass());
  }

  @Override
  protected synchronized void closeInternal() throws IOException {
    // close() can be called multiple times by super.commit() and super.close(), but we only want to emit metrics once
    if (!this.closed) {
      this.flush();
      this.orcFileWriter.close();
      this.closed = true;
      log.info("Emitting ORC event metrics");
      this.sendOrcWriterMetadataEvent();
      this.recycleRowBatchPool();
    } else {
      // Throw fatal exception if there's outstanding buffered data since there's risk losing data if proceeds.
      if (rowBatch.size > 0) {
        throw new CloseBeforeFlushException(this.inputSchema.toString());
      }
    }
  }

  private void sendOrcWriterMetadataEvent() {
    GobblinEventBuilder builder = new GobblinEventBuilder(ORC_WRITER_METRICS_NAME, ORC_WRITER_NAMESPACE);
    Map<String, String> eventMetadataMap = Maps.newHashMap();
    eventMetadataMap.put(METRICS_SCHEMA_NAME, this.inputSchema.getName());
    eventMetadataMap.put(METRICS_BYTES_WRITTEN, String.valueOf(this.bytesWritten()));
    eventMetadataMap.put(METRICS_RECORDS_WRITTEN, String.valueOf(this.recordsWritten()));
    eventMetadataMap.put(METRICS_BUFFER_RESIZES, String.valueOf(((GenericRecordToOrcValueWriter) this.valueWriter).getResizeCount()));
    eventMetadataMap.put(METRICS_BUFFER_SIZE, String.valueOf(this.batchSize));

    builder.addAdditionalMetadata(eventMetadataMap);
    EventSubmitter.submit(metricContext, builder);
  }
}
