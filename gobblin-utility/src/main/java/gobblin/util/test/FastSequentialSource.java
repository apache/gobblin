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

package gobblin.util.test;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import gobblin.configuration.SourceState;
import gobblin.configuration.WorkUnitState;
import gobblin.source.Source;
import gobblin.source.extractor.DataRecordException;
import gobblin.source.extractor.Extractor;
import gobblin.source.workunit.WorkUnit;

import lombok.RequiredArgsConstructor;


/**
 * A very simple {@link Source} that just emits long values counting up from 1 until either it reaches a user specified
 * value or a user specified time elapses.
 */
public class FastSequentialSource implements Source<String, Long> {


  public static final String NUM_WORK_UNITS = FastSequentialSource.class.getSimpleName() + ".numWorkUnits";
  public static final String MAX_RECORDS_PER_WORK_UNIT = FastSequentialSource.class.getSimpleName() + ".maxRecordsPerWorkUnit";
  public static final String MAX_SECONDS_PER_WORK_UNIT = FastSequentialSource.class.getSimpleName() + ".maxSecondsPerWorkUnit";

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    List<WorkUnit> workUnits = Lists.newArrayList();
    for (int i = 0; i < state.getPropAsInt(NUM_WORK_UNITS, 1); i++) {
      workUnits.add(new WorkUnit());
    }
    return workUnits;
  }

  @Override
  public Extractor<String, Long> getExtractor(WorkUnitState state) throws IOException {
    return new FastSequentialExtractor(state.getPropAsLong(MAX_RECORDS_PER_WORK_UNIT), state.getPropAsLong(MAX_SECONDS_PER_WORK_UNIT));
  }

  @Override
  public void shutdown(SourceState state) {

  }

  @RequiredArgsConstructor
  public static class FastSequentialExtractor implements Extractor<String, Long> {

    private final long maxRecords;
    private final long maxSeconds;

    private volatile long endTime;
    private volatile long recordNumber;

    @Override
    public String getSchema() throws IOException {
      return "schema";
    }

    @Override
    public Long readRecord(@Deprecated Long reuse) throws DataRecordException, IOException {
      if (this.endTime == 0) {
        this.endTime = System.currentTimeMillis() + this.maxSeconds * 1000;
      }
      if (System.currentTimeMillis() > this.endTime || this.recordNumber >= this.maxRecords) {
        return null;
      }
      return this.recordNumber++;
    }

    @Override
    public long getExpectedRecordCount() {
      return this.maxRecords;
    }

    @Override
    public long getHighWatermark() {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }
  }

}
