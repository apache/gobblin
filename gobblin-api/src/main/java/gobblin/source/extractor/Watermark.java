package gobblin.source.extractor;

import java.io.Serializable;
import java.util.List;

public interface Watermark extends Serializable {

  public Watermark getNewWatermark(List<Watermark> oldWatermark);
}
