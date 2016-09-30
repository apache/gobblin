package gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

import gobblin.annotation.Alias;

@Alias(value="DataFlowTopologyPickerByHadoopFsSource")
public class DataFlowTopologyPickerByHadoopFsSource implements DataFlowTopologyPickerBySource {
  
  @Override
  public Config getPreferredRoutes(Config allTopologies, EndPoint source) {
    Preconditions.checkArgument(source instanceof HadoopFsEndPoint,
        "source is NOT expectecd class " + HadoopFsEndPoint.class.getCanonicalName());
    
    HadoopFsEndPoint hadoopFsSource = (HadoopFsEndPoint)source;
    String clusterName = hadoopFsSource.getClusterName();
    
    Preconditions.checkArgument(allTopologies.hasPath(clusterName),
        "Can not find preferred topology for cluster name " + clusterName);
    return allTopologies.getConfig(clusterName);
  }
}
