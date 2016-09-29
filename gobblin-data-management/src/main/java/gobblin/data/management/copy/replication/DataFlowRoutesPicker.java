package gobblin.data.management.copy.replication;

import com.typesafe.config.Config;


/**
 * Used to pick preferred {@link DataFlowTopology.CopyRoute} in {@link com.typesafe.config.Config} format
 * @author mitu
 *
 */
public interface DataFlowRoutesPicker {

  public Config getPreferredRoutes();
}
