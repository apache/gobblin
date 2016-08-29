package gobblin.data.management.copy.replication;

import gobblin.data.management.copy.CopyableDataset;
import gobblin.dataset.DatasetsFinder;

public class ReplicationReplica extends AbstractReplicationData {

  @Override
  public boolean isSource() {
    return false;
  }

  public ReplicationReplica(String name, ReplicationLocation rl, DatasetsFinder<CopyableDataset> datasetFinder) {
    super(name, rl, datasetFinder);
  }
}
