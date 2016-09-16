package gobblin.data.management.copy.replication;

import org.testng.collections.Objects;

import com.typesafe.config.Config;

import gobblin.data.management.copy.CopyableDataset;


/**
 * Abstract class which has the basic implementations. It provides the implementation of 
 * <ul>
 *  <li>{@link #getReplicationName() getReplicationName}
 *  <li>{@link #getReplicationLocation() getReplicationLocation}
 *  <li>{@link #getDatasetClassName() getDatasetClassName}
 * </ul> 
 * 
 * The subclass need to implement {@link #isSource() isSource}
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

  //TODO
  public static String buildDataset(Config config) {
    return "";
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass()).add("isSource", this.isSource())
        .add("name", this.getReplicationName()).add("location", this.getReplicationLocation())
        .add("datasetClassName", this.datasetClassName).toString();
  }

}
