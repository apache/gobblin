package gobblin.data.management.copy.replication;

import gobblin.data.management.copy.CopyableDataset;


public class SourceEndPoint extends AbstractReplicationEndPoint {

  @Override
  public boolean isSource() {
    return true;
  }

  public SourceEndPoint(ReplicationLocation rl, CopyableDataset dataset) {
    super(ReplicationUtils.REPLICATION_SOURCE, rl, dataset);
  }

}
