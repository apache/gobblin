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

package org.apache.gobblin.cluster;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.runtime.JobShutdownException;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.AbstractSource;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * A source implementation that does nothing.
 */
public class DummySource extends AbstractSource<String, Integer> {

  private static final int NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR = 10;
  private static final int NUM_WORK_UNITS = 1;

  @Override
  public List<WorkUnit> getWorkunits(SourceState sourceState) {
    return Lists.newArrayList();
  }

  @Override
  public Extractor<String, Integer> getExtractor(WorkUnitState state)
      throws IOException {
    return new DummyExtractor(state);
  }

  @Override
  public void shutdown(SourceState state) {
    // Nothing to do
  }

  /**
   * A dummy implementation of {@link Extractor}.
   */
  private static class DummyExtractor implements Extractor<String, Integer> {

    private final WorkUnitState workUnitState;
    private int current;

    DummyExtractor(WorkUnitState workUnitState) {
      this.workUnitState = workUnitState;
      workUnitState.setProp("FOO", "BAR");
      this.current = 0;
    }

    @Override
    public String getSchema() {
      return "";
    }

    @Override
    public Integer readRecord(Integer reuse)
        throws DataRecordException, IOException {
      // Simply just get some records and stopped
      if (this.current > 10) {
        return null;
      }
      return this.current++;
    }

    @Override
    public long getExpectedRecordCount() {
      return DummySource.NUM_RECORDS_TO_EXTRACT_PER_EXTRACTOR;
    }

    @Override
    public long getHighWatermark() {
      return this.workUnitState.getHighWaterMark();
    }

    @Override
    public void close()
        throws IOException {
      // Nothing to do
    }

    @Override
    public void shutdown()
        throws JobShutdownException {
      // Nothing to do but overwrite unnecessary checking in the base interface.
    }
  }
}