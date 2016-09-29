package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Optional;

public interface EndPointCopyPairGenerator {
  
  // implied for pull mode
  public Optional<EndPointCopyPair> getCopyFrom(ReplicationConfiguration rc, EndPoint copyTo);
  
  // implied for push mode
  public Optional<List<EndPointCopyPair>> getCopyTo(ReplicationConfiguration rc, EndPoint copyFrom);
  
}
