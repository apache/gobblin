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

package org.apache.gobblin.temporal.ddm;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.eventbus.EventBus;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.instrumented.extractor.InstrumentedExtractor;
import org.apache.gobblin.source.InfiniteSource;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.gobblin.stream.RecordEnvelope;


/**
 * An implementation of {@link InfiniteSource} that does not generate any workunits. This is helpful when the {@link io.temporal.workflow.Workflow}
 * is driven by the job launcher and not the source. I.e. we want the discovery to be triggered on a {@link io.temporal.worker.Worker} and not the
 * {@link org.apache.gobblin.temporal.cluster.GobblinTemporalClusterManager}
 *
 * This class also implements the {@link InfiniteSource} to provide hooks for communicating with the
 * {@link org.apache.gobblin.temporal.joblauncher.GobblinTemporalJobLauncher} via the {@link EventBus}.
 */
@Slf4j
public class NoWorkUnitsInfiniteSource implements InfiniteSource {

  private final EventBus eventBus = new EventBus(this.getClass().getSimpleName());

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    return Arrays.asList(WorkUnit.createEmpty());
  }

  @Override
  public Extractor getExtractor(WorkUnitState state)
      throws IOException {
    // no-op stub extractor
    return new InstrumentedExtractor(state) {
      @Override
      public Object getSchema()
          throws IOException {
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

      @Override
      protected RecordEnvelope readRecordEnvelopeImpl() throws DataRecordException, IOException {
        return null;
      }
    };
  }

  @Override
  public void shutdown(SourceState state) {
  }

  @Override
  public boolean isEarlyStopped() {
    return InfiniteSource.super.isEarlyStopped();
  }

  @Override
  public EventBus getEventBus() {
    return this.eventBus;
  }
}
