package gobblin.data.management.copy.replication;

import org.testng.collections.Objects;

import gobblin.data.management.copy.CopyableDataset;


/**
 * Abstract class which has the basic implementations 
 * @author mitu
 *
 */

public abstract class AbstractReplicationEndPoint implements ReplicationEndPoint<CopyableDataset> {

  protected final String replicationName;
  protected final ReplicationLocation location;
  protected final CopyableDataset dataset;

  protected AbstractReplicationEndPoint(String replicationName, ReplicationLocation location, CopyableDataset dataset) {
    this.replicationName = replicationName;
    this.location = location;
    this.dataset = dataset;
  }

  @Override
  public String getReplicationName() {
    return this.replicationName;
  }

  @Override
  public ReplicationLocation getReplicationLocation() {
    return this.location;
  }

  @Override
  public CopyableDataset getDataset() {
    return this.dataset;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("isSource", this.isSource())
        .add("name", this.getReplicationName()).add("location", this.getReplicationLocation())
        .add("dataset", this.dataset.getClass().getName()).toString();
  }

}
