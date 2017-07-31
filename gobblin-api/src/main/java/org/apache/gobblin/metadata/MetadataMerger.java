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
package org.apache.gobblin.metadata;

import org.apache.gobblin.writer.FsWriterMetrics;


/**
 * Interface for an object that can merge metadata from several work units together.
 * @param <T> Type of the metadata record that will be merged
 */
public interface MetadataMerger<T> {
  /**
   * Process a metadata record, merging it with all previously processed records.
   * @param metadata Record to process
   */
  void update(T metadata);

  /**
   * Process a metrics record, merging it with all previously processed records.
   */
  void update(FsWriterMetrics metrics);

  /**
   * Get a metadata record that is a representation of all records passed into update().
   */
  T getMergedMetadata();
}
