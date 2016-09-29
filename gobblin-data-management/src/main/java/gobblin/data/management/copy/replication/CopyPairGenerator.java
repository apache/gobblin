package gobblin.data.management.copy.replication;

import java.util.Collection;

public interface CopyPairGenerator {
  
  public CopyPair generatePullRoute(ReplicationConfiguration rc, EndPoint copyTo);
  
  public Collection<CopyPair> generatePushRoute(ReplicationConfiguration rc, EndPoint copyFrom);

}
