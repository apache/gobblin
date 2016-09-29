package gobblin.data.management.copy.replication;

import java.util.List;

import com.typesafe.config.Config;

public class DataFlowTopology {
  
  private ReplicationCopyMode copyMode;
  private List<DataFlowPath> dataFlowPaths;

  public static class DataFlowPath{
    private final List<EndPoint> copyFroms;
    private final List<EndPoint> copyTos;
    
    public DataFlowPath(Config topologyConfig, ReplicationCopyMode copyMode, List<EndPoint> allEndPoints){
      if(copyMode==ReplicationCopyMode.PULL){
        
      }
      else {
        
      }
    }
  }
}
