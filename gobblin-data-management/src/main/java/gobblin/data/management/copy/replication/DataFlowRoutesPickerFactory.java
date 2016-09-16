package gobblin.data.management.copy.replication;

import com.typesafe.config.Config;

public class DataFlowRoutesPickerFactory {

  public static DataFlowRoutesPicker createDataFlowRoutesPicker(DataFlowRoutesPickerTypes type, Config allRoutes,
      SourceEndPoint source) {
    
    switch(type){
      case BY_SOURCE_CLUSTER:
        return new DataFlowRoutesPickerBySourceCluster(allRoutes, source);
    }
    
    throw new UnsupportedOperationException("Not support for DataFlowRoutesPicker type " + type); 
  }
}
