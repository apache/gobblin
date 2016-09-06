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
 *  <li>Specifying the {@link DatasetsFinder} which can find all data sets
 * </ul>
 * 
 * @author mitu
 *
 * @param <T>
 */
public interface ReplicationData<T extends CopyableDataset> {

  public boolean isSource();

  public String getReplicationName();

  public ReplicationLocation getReplicationLocation();

  public DatasetsFinder<T> getDatasetsFinder();
}
