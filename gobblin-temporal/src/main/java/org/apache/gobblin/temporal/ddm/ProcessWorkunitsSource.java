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


@Slf4j
public class ProcessWorkunitsSource implements InfiniteSource {

  private final EventBus eventBus = new EventBus(this.getClass().getSimpleName());

  public ProcessWorkunitsSource() {
  }

  @Override
  public List<WorkUnit> getWorkunits(SourceState state) {
    // PLACEHOLDER for now - This can be used to actually generate workunits for the workflow
    return Arrays.asList(WorkUnit.createEmpty());
  }

  @Override
  public Extractor getExtractor(WorkUnitState state)
      throws IOException {
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
