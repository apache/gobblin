package gobblin.data.management.copy.replication;

import gobblin.data.management.copy.CopyableDataset;


public class ReplicaEndPoint extends AbstractReplicationEndPoint {

  @Override
  public boolean isSource() {
    return false;
  }

  public ReplicaEndPoint(String name, ReplicationLocation rl, CopyableDataset dataset) {
    super(name, rl, dataset);
  }
}
