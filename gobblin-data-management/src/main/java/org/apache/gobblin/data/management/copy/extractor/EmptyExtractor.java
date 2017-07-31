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

package org.apache.gobblin.data.management.copy.extractor;

import lombok.AllArgsConstructor;

import java.io.IOException;

import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;


/**
 * An extractor that returns no records.
 */
@AllArgsConstructor
public class EmptyExtractor<S, D> implements Extractor<S, D> {

  private final S schema;

  @Override public S getSchema() throws IOException {
    return this.schema;
  }

  @Override public D readRecord(@Deprecated D reuse) throws DataRecordException, IOException {
    return null;
  }

  @Override public long getExpectedRecordCount() {
    return 0;
  }

  @Override public long getHighWatermark() {
    return 0;
  }

  @Override public void close() throws IOException {}
}
