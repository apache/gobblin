package gobblin.data.management.copy.replication;

import java.util.List;

import lombok.Getter;

public class DataFlowTopology {
  
  @Getter
  private List<Routes> routes;

  public static class Routes{
    @Getter
    private ReplicationReplica copyDestination;
    
    @Getter
    private List<ReplicationReplica> copFrom;
  }
}
