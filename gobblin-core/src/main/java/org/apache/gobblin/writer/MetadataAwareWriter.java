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

import org.apache.gobblin.metadata.types.GlobalMetadata;


/**
 * Represents a Writer that is metadata aware. Metadata aware writers
 * specify default {@link GlobalMetadata} that will be added to all records that flow
 * through the pipeline they are a part of.
 *
 * This allows a writer to communicate with the MetadataWriterWrapper that surrounds it
 * and is the one responsible for processing any metadata that flows through the conversion pipeline.
 */
public interface MetadataAwareWriter {
  /**
   * Get default metadata that will be attached to records that pass through this writer.
   * For example, if the writer always gzip's data that passes through it, it would implement
   * this method and return a new GlobalMetadata object record with transferEncoding: ['gzip'].
   *
   * This default metadata will be merged with the RecordWithMetadata that is added to each record
   * by the source and converters in the pipeline.
   */
  GlobalMetadata getDefaultMetadata();
}
