package gobblin.source.extractor;

import java.io.IOException;

import gobblin.configuration.WorkUnitState;
import gobblin.instrumented.extractor.InstrumentedExtractor;

/**
 * Dummy extractor that always returns 0 records.
 */
public class DummyExtractor<S, D> extends InstrumentedExtractor<S, D> {

  public DummyExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public S getSchema() throws IOException {
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
  public D readRecordImpl(D reuse) throws DataRecordException, IOException {
    return null;
  }
}
