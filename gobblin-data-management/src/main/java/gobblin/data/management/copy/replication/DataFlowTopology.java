package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Class to represent the data flow topology from copy source to copy destinations. Each {@link DataFlowTopology} contains
 * a list of {@link DataFlowTopology.DataFlowPath}s
 *
 *
 */

@Data
public class DataFlowTopology {
  
  private List<DataFlowPath> dataFlowPaths = new ArrayList<>();

  public void addDataFlowPath(DataFlowPath p){
    this.dataFlowPaths.add(p);
  }
  
  @AllArgsConstructor
  @Data
  public static class DataFlowPath{
    private List<CopyRoute> copyPairs;
  }
}
