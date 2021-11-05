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

package org.apache.gobblin.hive.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

import org.apache.gobblin.hive.spec.HiveSpec;
import org.apache.gobblin.metadata.GobblinMetadataChangeEvent;
import org.apache.gobblin.stream.RecordEnvelope;


/**
 * This is the interface that is used to calculate and accumulate the desired metadata and register to the metadata store
 */
public interface MetadataWriter extends Closeable {
  String CACHE_EXPIRING_TIME = "GMCEWriter.cache.expiring.time.hours";
  int DEFAULT_CACHE_EXPIRING_TIME = 1;

  /**
   * Register the metadata of specific table to the metadata store. This is a blocking method,
   * meaning once it returns, as long as the underlying metadata storage is transactional (e.g. Mysql as for HMS),
   * one could expect the metadata registration going through and being persisted already.
   *
   * @param dbName The db name of metadata-registration target.
   * @param tableName The table name of metadata-registration target.
   * @throws IOException
   */
  void flush(String dbName, String tableName) throws IOException;

  /**
   * If something wrong happens, we want to clean up in-memory state for the table inside the writer so that we can continue
   * registration for this table without affect correctness
   *
   * @param dbName The db name of metadata-registration target.
   * @param tableName The table name of metadata-registration target.
   * @throws IOException
   */
  void reset(String dbName, String tableName) throws IOException;

  /**
   * Compute and cache the metadata from the GMCE
   * @param recordEnvelope Containing {@link GobblinMetadataChangeEvent}
   * @param newSpecsMap The container (as a map) for new specs.
   * @param oldSpecsMap The container (as a map) for old specs.
   * @param tableSpec A sample table spec representing one instances among all path's {@link HiveSpec}
   * @throws IOException
   */
  void writeEnvelope(RecordEnvelope<GenericRecord> recordEnvelope, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException;
}
