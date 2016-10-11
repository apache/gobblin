package gobblin.data.management.copy.replication;

import java.util.Comparator;

import com.google.common.base.Optional;

import gobblin.source.extractor.ComparableWatermark;

public class CopyRouteComparatorBySourceWatermark implements Comparator<CopyRoute>{

  @Override
  public int compare(CopyRoute o1, CopyRoute o2) {
    EndPoint from1 = o1.getCopyFrom();
    EndPoint from2 = o2.getCopyFrom();
    Optional<ComparableWatermark> w1 = from1.getWatermark();
    Optional<ComparableWatermark> w2 = from2.getWatermark();
    
    // both are absent
    if(!w1.isPresent() && !w2.isPresent()){
      return 0;
    }
    
    if(!w2.isPresent()){
      return 1;
    }
    
    if(!w1.isPresent()){
      return -1;
    }
    
    return w1.get().compareTo(w2.get());
  }
}
