package gobblin.data.management.copy.replication;

import java.util.List;

import com.google.common.base.Optional;

public interface CopyRouteGenerator {
  
  // implied for pull mode
  public Optional<CopyRoute> getPullRoute(ReplicationConfiguration rc, EndPoint copyTo);
  
  // implied for push mode
  public Optional<List<CopyRoute>> getPushRoutes(ReplicationConfiguration rc, EndPoint copyFrom);
  
}
