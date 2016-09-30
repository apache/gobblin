package gobblin.data.management.copy.replication;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import lombok.Data;
import lombok.AllArgsConstructor;;

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
  
  @Override
  public String toString(){
    Function<DataFlowPath, String> func =
        new Function<DataFlowPath, String>() {
          @Override
          public String apply(DataFlowPath t) {
            return t.toString();
          }
        };
        
    return Objects.toStringHelper(this.getClass())
        .add("dataFlows:", Joiner.on(",").join(Lists.transform(this.dataFlowPaths, func))).toString();
  }
  
  @Data
  @AllArgsConstructor
  public static class DataFlowPath{
    private List<CopyRoute> copyPairs;
    
    @Override
    public String toString(){
      Function<CopyRoute, String> func =
          new Function<CopyRoute, String>() {
            @Override
            public String apply(CopyRoute t) {
              return t.toString();
            }
          };
          
      return Objects.toStringHelper(this.getClass())
          .add("copyPairs:", Joiner.on(",").join(Lists.transform(this.copyPairs, func))).toString();
    }
  }
}
