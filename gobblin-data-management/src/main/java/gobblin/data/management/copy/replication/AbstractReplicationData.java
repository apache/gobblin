package gobblin.data.management.copy.replication;

import gobblin.data.management.copy.CopyableDataset;
import gobblin.dataset.DatasetsFinder;

/**
 * Abstract class which has the basic implementations 
 * @author mitu
 *
 */

public abstract class AbstractReplicationData implements ReplicationData<CopyableDataset> {

  protected final String replicationName;
  protected final ReplicationLocation location;
  protected final DatasetsFinder<CopyableDataset> datasetFinder;

  protected AbstractReplicationData(String replicationName, ReplicationLocation location, DatasetsFinder<CopyableDataset> datasetFinder ) {
    this.replicationName = replicationName;
    this.location = location;
    this.datasetFinder = datasetFinder;
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
  public DatasetsFinder<CopyableDataset> getDatasetsFinder(){
    return this.datasetFinder;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("isSource:" + this.isSource() + ", ");
    sb.append("name:" + this.getReplicationName() + ", ");
    sb.append("location:[" + this.getReplicationLocation() + "], ");
    //TODO
    if(this.datasetFinder==null){
      sb.append("dataset finder :null");
    }
    else{
      sb.append("dataset finder :" + this.datasetFinder.getClass().getName());
    }
    return sb.toString();
  }

}
