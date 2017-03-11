package gobblin.source.extractor.partition;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


@AllArgsConstructor
@EqualsAndHashCode
public class Partition {
  @Getter private long lowWatermark;
  @Getter private long highWatermark;

  // Indicate if the Partition highWatermark is set as user specifies, not computed on the fly
  private boolean hasUserSpecifiedHighWatermark;

  public Partition(long lowWatermark, long highWatermark) {
    this(lowWatermark, highWatermark, false);
  }

  public boolean getHasUserSpecifiedHighWatermark() {
    return hasUserSpecifiedHighWatermark;
  }
}
