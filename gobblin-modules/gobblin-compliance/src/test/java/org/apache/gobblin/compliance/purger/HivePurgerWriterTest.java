package org.apache.gobblin.compliance.purger;

import java.io.IOException;

import org.mockito.Mockito;
import org.testng.annotations.Test;


@Test
public class HivePurgerWriterTest {
  private PurgeableHivePartitionDataset datasetMock = Mockito.mock(PurgeableHivePartitionDataset.class);
  private HivePurgerWriter writerMock = Mockito.mock(HivePurgerWriter.class);

  public void purgeTest()
      throws IOException {
    Mockito.doCallRealMethod().when(this.writerMock).write(this.datasetMock);
    this.writerMock.write(this.datasetMock);
    Mockito.verify(this.datasetMock).purge();
  }
}
