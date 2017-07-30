package gobblin.data.management.conversion.hive;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

import gobblin.configuration.WorkUnitState;
import gobblin.data.management.conversion.hive.publisher.HiveConvertPublisher;


/**
 * @author adsharma
 */
@Test
public class CopyPartitionParametersTest {
  private static final String SRC_PARTITION = "srcDb@srcTable@srcPartition";
  private static final String DEST_PARTITION = "destDb@destTable@destPartition";
  private static final String ALL = "*";
  private static final String MY_VALUE = "myValue";
  private static final String MY_PROP = "myProp";

  private HiveConvertPublisher publisherMock = Mockito.mock(HiveConvertPublisher.class);
  private WorkUnitState workUnitState = new WorkUnitState();

  private Partition sourcePartition = Mockito.mock(Partition.class);
  private Partition destPartition = Mockito.mock(Partition.class);

  private Map<String, String> sourceParams = new HashMap<>();
  private Map<String, String> destParams = new HashMap<>();

  @BeforeTest
  private void init() {
    Mockito.doReturn(Optional.fromNullable(this.sourcePartition)).when(this.publisherMock)
        .getPartitionObject(SRC_PARTITION);
    Mockito.doReturn(Optional.fromNullable(this.destPartition)).when(this.publisherMock)
        .getPartitionObject(DEST_PARTITION);
    Mockito.doReturn(true).when(this.publisherMock).dropPartition(DEST_PARTITION);
    Mockito.doReturn(true).when(this.publisherMock).addPartition(this.destPartition, DEST_PARTITION);
    Mockito.doCallRealMethod().when(this.publisherMock)
        .copyPartitionParams(SRC_PARTITION, DEST_PARTITION, Collections.singletonList(ALL), Collections.EMPTY_LIST);
    Mockito.doCallRealMethod().when(this.publisherMock)
        .preservePartitionParams(Collections.singleton(this.workUnitState));

    Mockito.doReturn(this.sourceParams).when(this.sourcePartition).getParameters();
    Mockito.doReturn(this.destParams).when(this.destPartition).getParameters();
  }

  public void test() {
    this.workUnitState.setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    this.workUnitState.setProp(HiveConvertPublisher.COMPLETE_SOURCE_PARTITION_NAME, SRC_PARTITION);
    this.workUnitState.setProp(HiveConvertPublisher.COMPLETE_DEST_PARTITION_NAME, DEST_PARTITION);
    this.workUnitState.setProp(HiveConvertPublisher.PARTITION_PARAMETERS_WHITELIST, ALL);

    this.sourceParams.put(MY_PROP, MY_VALUE);
    this.publisherMock.preservePartitionParams(Collections.singleton(this.workUnitState));
    Assert.assertTrue(this.destParams.get(MY_PROP).equalsIgnoreCase(MY_VALUE));
  }
}
