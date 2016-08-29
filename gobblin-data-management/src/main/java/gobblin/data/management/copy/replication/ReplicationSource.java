package gobblin.data.management.copy.replication;

import gobblin.data.management.copy.CopyableDataset;
import gobblin.dataset.DatasetsFinder;


public class ReplicationSource extends AbstractReplicationData {

  @Override
  public boolean isSource() {
    return true;
  }

  public ReplicationSource(ReplicationLocation rl, DatasetsFinder<CopyableDataset> datasetFinder) {
    super(ReplicationUtils.REPLICATION_SOURCE, rl, datasetFinder);
  }

}
