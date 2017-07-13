/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.writer;

import java.io.IOException;
import java.util.Map;

import gobblin.annotation.Alpha;
import gobblin.source.extractor.CheckpointableWatermark;
import gobblin.stream.RecordEnvelope;


/**
 * A DataWriter that is WatermarkAware. Required for implementing writers that
 * can operate in streaming mode.
 */
@Alpha
public interface WatermarkAwareWriter<D> extends DataWriter<D> {

  /**
   *
   * @return true if the writer can support watermark-bearing record envelopes
   */
  boolean isWatermarkCapable();

  /**
   * Write a record (possibly asynchronously), ack the envelope on success.
   * @param recordEnvelope: a container for the record and the acknowledgable watermark
   * @throws IOException: if this write (or preceding write failures) have caused a fatal exception.
   */
  void writeEnvelope(RecordEnvelope<D> recordEnvelope) throws IOException;

  /**
   * @return A Watermark per source that can safely be committed because all records associated with it
   * and earlier watermarks have been committed to the destination. Return empty if no such watermark exists.
   *
   */
  @Deprecated
  Map<String, CheckpointableWatermark> getCommittableWatermark();

  /**
   *
   * @return The lowest watermark out of all pending write requests
   */
  @Deprecated
  Map<String, CheckpointableWatermark> getUnacknowledgedWatermark();

}
