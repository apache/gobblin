package gobblin.source.extractor.partition;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;


@AllArgsConstructor
@EqualsAndHashCode
public class Partition {
  @Getter
  private long lowWatermark;
  @Getter private long highWatermark;
  private boolean hasUserSpecifiedHighWatermark;

  public Partition(long lowWatermark, long highWatermark) {
    this(lowWatermark, highWatermark, false);
  }

  public boolean getHasUserSpecifiedHighWatermark() {
    return hasUserSpecifiedHighWatermark;
  }
}
