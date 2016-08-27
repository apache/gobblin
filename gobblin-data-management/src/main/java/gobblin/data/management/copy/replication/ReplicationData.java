package gobblin.data.management.copy.replication;

import com.google.common.base.Optional;

import gobblin.dataset.Dataset;
import gobblin.dataset.DatasetsFinder;

/**
 * Interface Replication Data is used to describe the data which need to be replicated.
 * 
 * A ReplicationData is responsible for:
 * <ul>
 *  <li>Telling whether the data is source or replicas
 *  <li>Specifying the replication name
 *  <li>Specifying the replication location {@link ReplicationLocation}
 * </ul>
 * 
 * @author mitu
 *
 * @param <T>
 */
public interface ReplicationData<T extends Dataset> {
  
  public boolean isSource();
  
  public String getReplicationName();
  
  public ReplicationLocation getReplicationLocation();
  
  public Optional<DatasetsFinder<T>> getDatasetsFinder();
}
