package gobblin.data.management.copy.replication;

public class ReplicaEndPoint extends AbstractReplicationEndPoint {

  @Override
  public boolean isSource() {
    return false;
  }

  public ReplicaEndPoint(String name, ReplicationLocation rl, String dataset) {
    super(name, rl, dataset);
  }
}
