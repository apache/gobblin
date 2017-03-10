package gobblin.source.extractor.partition;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
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

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj instanceof Partition) {
      Partition partition = (Partition) obj;
      return lowWatermark == partition.getLowWatermark() && highWatermark == partition.getHighWatermark()
          && hasUserSpecifiedHighWatermark == partition.getHasUserSpecifiedHighWatermark();
    }
    return false;
  }
}
