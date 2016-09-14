package gobblin.data.management.copy.replication;

import gobblin.data.management.copy.CopyableDataset;
import gobblin.dataset.DatasetsFinder;


/**
 * Interface Replication Data is used to describe the data which need to be replicated.
 * 
 * A ReplicationData is responsible for:
 * <ul>
 *  <li>Telling whether the data is source or replicas
 *  <li>Specifying the replication name
 *  <li>Specifying the replication location {@link ReplicationLocation}
 *  <li>Specifying the {@link Dataset} class name
 * </ul>
 * 
 * @author mitu
 */
public interface ReplicationEndPoint<T extends CopyableDataset> {

  /**
   * @return true iff this ReplicationEndPoint is the original source
   */
  public boolean isSource();

  /**
   * @return the replication name, for each dataset, the replication name should be unique
   */
  public String getReplicationName();

  /**
   * 
   * @return the {@link ReplicationLocation} of this 
   */
  public ReplicationLocation getReplicationLocation();

  public String getDatasetClassName();
}
