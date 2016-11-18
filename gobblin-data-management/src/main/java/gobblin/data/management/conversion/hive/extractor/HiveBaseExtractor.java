package gobblin.data.management.conversion.hive.extractor;

import java.io.IOException;
import gobblin.source.extractor.Extractor;


/**
 * Base {@link Extractor} for extracting from {@link gobblin.data.management.conversion.hive.source.HiveSource}
 */
public abstract class HiveBaseExtractor<S, D> implements Extractor<S, D> {


  @Override
  public long getExpectedRecordCount() {
    return 1;
  }

  /**
   * Watermark is not managed by this extractor.
   */
  @Override
  public long getHighWatermark() {
    return 0;
  }

  @Override
  public void close() throws IOException {}
}
