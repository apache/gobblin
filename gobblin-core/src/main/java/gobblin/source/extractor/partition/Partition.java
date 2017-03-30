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
 *
 *   By default, {@link #isLowWatermarkInclusive} is true, {@link #isHighWatermarkInclusive} is false,
 *   {@link #hasUserSpecifiedHighWatermark} is false
 * </p>
 *
 * @author zhchen
 */
@AllArgsConstructor
@EqualsAndHashCode
public class Partition {
  public static final String IS_LOWWATERMARK_INCLUSIVE = "partition.isLowWatermarkInclusive";
  public static final String IS_HIGHWATERMARK_INCLUSIVE = "partition.isLowWatermarkInclusive";

  @Getter
  private final long lowWatermark;
  @Getter
  private final boolean isLowWatermarkInclusive;
  @Getter
  private final long highWatermark;
  @Getter
  private final boolean isHighWatermarkInclusive;

  /**
   * Indicate if the Partition highWatermark is set as user specifies, not computed on the fly
   */
  private final boolean hasUserSpecifiedHighWatermark;

  public Partition(long lowWatermark, long highWatermark) {
    this(lowWatermark, true, highWatermark, false, false);
  }

  public Partition(long lowWatermark, long highWatermark, boolean isHighWatermarkInclusive,
      boolean hasUserSpecifiedHighWatermark) {
    this(lowWatermark, true, highWatermark, isHighWatermarkInclusive, hasUserSpecifiedHighWatermark);
  }

  public Partition(long lowWatermark, long highWatermark, boolean hasUserSpecifiedHighWatermark) {
    this(lowWatermark, true, highWatermark, false, hasUserSpecifiedHighWatermark);
  }

  public boolean getHasUserSpecifiedHighWatermark() {
    return hasUserSpecifiedHighWatermark;
  }
}
