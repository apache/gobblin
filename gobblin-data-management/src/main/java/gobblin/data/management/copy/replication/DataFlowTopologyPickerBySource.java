package gobblin.data.management.copy.replication;

import com.typesafe.config.Config;

/**
 * Used to pick preferred {@link DataFlowTopology} in {@link com.typesafe.config.Config} format
 * @author mitu
 *
 */
public interface DataFlowTopologyPickerBySource {
  
  public Config getPreferredRoutes(Config allDataFlowTopologies, EndPoint source);
  
}
