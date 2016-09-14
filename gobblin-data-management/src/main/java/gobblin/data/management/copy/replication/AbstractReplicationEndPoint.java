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
  protected final String datasetClassName;

  protected AbstractReplicationEndPoint(String replicationName, ReplicationLocation location, String dataset) {
    this.replicationName = replicationName;
    this.location = location;
    this.datasetClassName = dataset;
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
  public String getDatasetClassName() {
    return this.datasetClassName;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("isSource", this.isSource())
        .add("name", this.getReplicationName()).add("location", this.getReplicationLocation())
        .add("datasetClassName", this.datasetClassName).toString();
  }

}
