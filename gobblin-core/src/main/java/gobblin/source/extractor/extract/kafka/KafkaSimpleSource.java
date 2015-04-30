package gobblin.source.extractor.extract.kafka;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;

import java.io.IOException;

/**
 * Created by akshaynanavati on 4/30/15.
 */
public class KafkaSimpleSource extends KafkaSource<String, byte[]> {
  /**
   * Get an {@link Extractor} based on a given {@link WorkUnitState}.
   * <p/>
   * <p>
   * The {@link Extractor} returned can use {@link WorkUnitState} to store arbitrary key-value pairs
   * that will be persisted to the state store and loaded in the next scheduled job run.
   * </p>
   *
   * @param state a {@link WorkUnitState} carrying properties needed by the returned {@link Extractor}
   * @return an {@link Extractor} used to extract schema and data records from the data source
   * @throws IOException if it fails to create an {@link Extractor}
   */
  @Override
  public Extractor<String, byte[]> getExtractor(WorkUnitState state) throws IOException {
    return new KafkaSimpleExtractor(state);
  }
}
