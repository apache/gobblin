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
import org.apache.gobblin.stream.RecordEnvelope;


/**
 * This is the interface of the writer which is used to calculate and accumulate the desired metadata and register to the metadata store
 */
public interface MetadataWriter extends Closeable {
  String CACHE_EXPIRING_TIME = "GMCEWriter.cache.expiring.time.hours";
  int DEFAULT_CACHE_EXPIRING_TIME = 1;

  /*
  Register the metadata of specific table to the metadata store
   */
  public void flush(String dbName, String tableName) throws IOException;

  /*
  Compute and cache the metadata from the GMCE
   */
  public void writeEnvelope(RecordEnvelope<GenericRecord> recordEnvelope, Map<String, Collection<HiveSpec>> newSpecsMap,
      Map<String, Collection<HiveSpec>> oldSpecsMap, HiveSpec tableSpec) throws IOException;
}
