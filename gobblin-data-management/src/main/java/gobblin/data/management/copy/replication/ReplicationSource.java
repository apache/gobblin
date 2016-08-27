package gobblin.data.management.copy.replication;

import com.google.common.base.Optional;

import gobblin.dataset.DatasetsFinder;

public class ReplicationSource extends AbstractReplicationData {

  @Override
  public boolean isSource() {
    return true;
  }

  @Override
  public Optional getDatasetsFinder() {
    return this.datasetsFinder;
  }

  private final Optional datasetsFinder;

  public ReplicationSource(ReplicationLocation rl, Optional ds){
    super(ReplicationConfiguration.REPLICATION_SOURCE, rl);
    this.datasetsFinder = ds;
  }

}
