package gobblin.data.management.copy.replication;

import com.google.common.base.Optional;

public class ReplicationReplica extends AbstractReplicationData{

  @Override
  public boolean isSource() {
    return false;
  }

  @Override
  public Optional getDatasetsFinder() {
    return this.datasetsFinder;
    
  }
  
  private final Optional datasetsFinder;
  
  public ReplicationReplica(String name, ReplicationLocation rl, Optional ds){
    super(name, rl);
    this.datasetsFinder = ds;
  }
}
