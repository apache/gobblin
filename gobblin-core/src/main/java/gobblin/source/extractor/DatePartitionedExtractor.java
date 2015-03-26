package gobblin.source.extractor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.hadoop.HadoopExtractor;


/**
 * Extension of {@link HadoopExtractor} where the {@link #getHighWatermark()} method returns the result of the
 * specified WorkUnit's {@link gobblin.source.workunit.WorkUnit#getHighWaterMark()} method.
 */
public class DatePartitionedExtractor extends HadoopExtractor<Schema, GenericRecord> {

  public DatePartitionedExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  /**
   * Returns the HWM of the workUnit
   * {@inheritDoc}
   * @see gobblin.source.extractor.filebased.FileBasedExtractor#getHighWatermark()
   */
  @Override
  public long getHighWatermark() {
    return this.workUnit.getHighWaterMark();
  }
}
