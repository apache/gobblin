package gobblin.data.management.copy.replication;

import com.google.common.base.Preconditions;
import com.typesafe.config.Config;


public class SourceEndPoint extends AbstractReplicationEndPoint {

  @Override
  public boolean isSource() {
    return true;
  }

  public SourceEndPoint(ReplicationLocation rl, String dataset) {
    super(ReplicationConfiguration.REPLICATION_SOURCE, rl, dataset);
  }

  public static SourceEndPoint buildSource(Config config) {
    Preconditions.checkArgument(config.hasPath(ReplicationConfiguration.REPLICATION_SOURCE),
        "missing required config entery " + ReplicationConfiguration.REPLICATION_SOURCE);

    Config sourceConfig = config.getConfig(ReplicationConfiguration.REPLICATION_SOURCE);

    return new SourceEndPoint(ReplicationLocationFactory.buildReplicationLocation(sourceConfig),
        AbstractReplicationEndPoint.buildDataset(sourceConfig));
  }

}
