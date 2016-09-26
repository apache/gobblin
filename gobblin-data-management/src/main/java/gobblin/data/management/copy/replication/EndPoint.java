package gobblin.data.management.copy.replication;

import gobblin.source.extractor.ComparableWatermark;
import gobblin.source.extractor.extract.LongWatermark;

public interface EndPoint {

  public boolean isSource();
  
  public String getEndPointName();
  
  public LongWatermark getWatermark();
}
