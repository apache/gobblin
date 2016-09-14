package gobblin.data.management.copy.replication;

public class SourceEndPoint extends AbstractReplicationEndPoint {

  @Override
  public boolean isSource() {
    return true;
  }

  public SourceEndPoint(ReplicationLocation rl, String dataset) {
    super(ReplicationUtils.REPLICATION_SOURCE, rl, dataset);
  }

}
