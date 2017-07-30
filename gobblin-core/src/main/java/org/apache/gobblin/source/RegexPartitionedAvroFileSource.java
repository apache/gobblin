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
package gobblin.source;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.hadoop.AvroFileExtractor;


public class RegexPartitionedAvroFileSource extends PartitionedFileSourceBase<Schema, GenericRecord> {
  public RegexPartitionedAvroFileSource() {
    super(new RegexBasedPartitionedRetriever(".avro"));
  }

  @Override
  public Extractor<Schema, GenericRecord> getExtractor(WorkUnitState state)
      throws IOException {
    return new AvroFileExtractor(state);
  }
}
