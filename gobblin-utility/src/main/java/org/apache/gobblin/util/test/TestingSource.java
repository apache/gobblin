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
package org.apache.gobblin.util.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;

import lombok.Getter;
import lombok.Setter;

/**
 * A trivial implementation of Source to be used to testing.
 */
public class TestingSource implements Source<String, String> {
  @Setter @Getter protected List<WorkUnit> _workunits = new ArrayList<>();

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    return _workunits;
  }

  @Override
  public Extractor<String, String> getExtractor(WorkUnitState state) throws IOException {
    return new Extract();
  }

  @Override
  public void shutdown(SourceState state) {
    // Nothing to do
  }

  public static class Extract implements Extractor<String, String> {
    @Override public void close() throws IOException {
      // Nothing to do
    }

    @Override
    public String getSchema() throws IOException {
      return "none";
    }

    @Override
    public String readRecord(String reuse) throws DataRecordException, IOException {
      return null;
    }

    @Override
    public long getExpectedRecordCount() {
      return 0;
    }

    @Override
    public long getHighWatermark() {
      return 0;
    }

  }

}
