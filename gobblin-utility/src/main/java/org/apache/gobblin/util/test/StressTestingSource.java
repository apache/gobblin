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
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.Source;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.Extract.TableType;
import org.apache.gobblin.source.workunit.WorkUnit;

/**
 * A {@link Source} to be used for stress testing
 *
 * This source uses an extractor that can be configured to have sleep and computation time before returning a record.
 * The size of the returned record can also be configured.
 */
public class StressTestingSource implements Source<String, byte[]> {
  public static final String CONFIG_NAMESPACE = "stressTest";
  public static final String NUM_WORK_UNITS_KEY = CONFIG_NAMESPACE + "." + "numWorkUnits";
  public static final int DEFAULT_NUM_WORK_UNITS = 1;
  public static final String RUN_DURATION_KEY = CONFIG_NAMESPACE + "." + "runDurationSecs";
  public static final int DEFAULT_RUN_DURATION = 0;
  public static final String COMPUTE_TIME_MICRO_KEY = CONFIG_NAMESPACE + "." + "computeTimeMicro";
  public static final int DEFAULT_COMPUTE_TIME_MICRO = 0;
  public static final String SLEEP_TIME_MICRO_KEY = CONFIG_NAMESPACE + "." + "sleepTimeMicro";
  public static final int DEFAULT_SLEEP_TIME = 0;
  public static final String NUM_RECORDS_KEY = CONFIG_NAMESPACE + "." + "numRecords";
  public static final int DEFAULT_NUM_RECORDS = 1;
  public static final String MEM_ALLOC_BYTES_KEY = CONFIG_NAMESPACE + "." + "memAllocBytes";
  public static final int DEFAULT_MEM_ALLOC_BYTES = 8;

  private static final long INVALID_TIME = -1;

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    int numWorkUnits = state.getPropAsInt(NUM_WORK_UNITS_KEY, DEFAULT_NUM_WORK_UNITS);

    Extract extract = new Extract(TableType.APPEND_ONLY,
        StressTestingSource.class.getPackage().getName(),
        StressTestingSource.class.getSimpleName());

    List<WorkUnit> wus = new ArrayList<>(numWorkUnits);

    for (int i = 1; i <= numWorkUnits; ++i) {
      WorkUnit wu = new WorkUnit(extract);
      wus.add(wu);
    }

    return wus;
  }

  @Override
  public Extractor<String, byte[]> getExtractor(WorkUnitState state) {
    return new ExtractorImpl(state);
  }

  @Override
  public void shutdown(SourceState state) {
    // Nothing to do
  }

  public static class ExtractorImpl implements Extractor<String, byte[]> {
    private int recordsEmitted = 0;
    private final long startTime;
    private final long endTime;
    private final int computeTimeNano;
    private final int sleepTimeMicro;
    private final int numRecords;
    private final int memAllocBytes;
    private final Random random;

    public ExtractorImpl(WorkUnitState state) {
      this.random = new Random();
      this.startTime = System.currentTimeMillis();

      int runDuration = state.getPropAsInt(RUN_DURATION_KEY, DEFAULT_RUN_DURATION);

      // set the end time based on the configured duration
      if (runDuration > 0) {
        this.endTime = this.startTime + runDuration * 1000;
      } else {
        this.endTime = INVALID_TIME;
      }

      this.computeTimeNano = state.getPropAsInt(COMPUTE_TIME_MICRO_KEY, DEFAULT_COMPUTE_TIME_MICRO) * 1000;
      this.sleepTimeMicro = state.getPropAsInt(SLEEP_TIME_MICRO_KEY, DEFAULT_SLEEP_TIME);
      // num records only takes effect if the duration is not specified
      this.numRecords = this.endTime == INVALID_TIME ? state.getPropAsInt(NUM_RECORDS_KEY, DEFAULT_NUM_RECORDS) : 0;
      this.memAllocBytes = state.getPropAsInt(MEM_ALLOC_BYTES_KEY, DEFAULT_MEM_ALLOC_BYTES);
    }

    @Override
    public void close() throws IOException {
      // Nothing to do
    }

    @Override
    public String getSchema() throws IOException {
      return "string";
    }

    /**
     * Read a record with configurable idle and compute time.
     **/
    @Override
    public byte[] readRecord(byte[] reuse) throws DataRecordException, IOException {

      // If an end time is configured then it is used as the stopping point otherwise the record count limit is used
      if ((this.endTime != INVALID_TIME && System.currentTimeMillis() > this.endTime) ||
          (this.numRecords > 0 && this.recordsEmitted >= this.numRecords)) {
        return null;
      }

      // spend time computing
      if (this.computeTimeNano > 0) {
        final long startComputeNanoTime = System.nanoTime();
        final byte[] bytes = new byte[100];

        while (System.nanoTime() - startComputeNanoTime < this.computeTimeNano) {
          random.nextBytes(bytes);
        }
      }

      // sleep
      if (this.sleepTimeMicro > 0) {
        try {
          TimeUnit.MICROSECONDS.sleep(this.sleepTimeMicro);
        } catch (InterruptedException e) {
        }
      }

      this.recordsEmitted++;

      return newMessage(this.memAllocBytes);
    }

    @Override public long getExpectedRecordCount() {
      return this.numRecords;
    }

    @Override public long getHighWatermark() {
      return 0;
    }

    /**
     * Create a message of numBytes size.
     * @param numBytes number of bytes to allocate for the message
     */
    private byte[] newMessage(int numBytes) {
      byte[] stringBytes = String.valueOf(this.recordsEmitted).getBytes(Charsets.UTF_8);

      return Arrays.copyOf(stringBytes, numBytes);
    }
  }
}
