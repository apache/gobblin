package gobblin.source.extractor.partition;

import gobblin.source.Source;
import gobblin.source.workunit.WorkUnit;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


/**
 * This class encapsulates the two ends, {@link #lowWatermark} and {@link #highWatermark}, of a partition and some
 * metadata, e.g. {@link #hasUserSpecifiedHighWatermark}, to describe the partition.
 *
 * <p>
 *   A {@link Source} partitions its data into a collection of {@link Partition}, each of which will be used to create
 *   a {@link WorkUnit}.
 * </p>
 *
 * @author zhchen
 */
@AllArgsConstructor
@EqualsAndHashCode
public class Partition {
  @Getter private final long lowWatermark;
  @Getter private final long highWatermark;

  /**
   * Indicate if the Partition highWatermark is set as user specifies, not computed on the fly
   */
  private final boolean hasUserSpecifiedHighWatermark;

  public Partition(long lowWatermark, long highWatermark) {
    this(lowWatermark, highWatermark, false);
  }

  public boolean getHasUserSpecifiedHighWatermark() {
    return hasUserSpecifiedHighWatermark;
  }
}
