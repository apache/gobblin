package gobblin.policies.count;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.qualitychecker.task.TaskLevelPolicy;
import gobblin.qualitychecker.task.TaskLevelPolicy.Result;

import org.testng.Assert;
import org.testng.annotations.Test;


public class RowCountRangePolicyTest {

  @Test
  public void testRangePolicyFailure() {
    RowCountRangePolicy rangePolicy = new RowCountRangePolicy(getTestState(4, 1, 0.5), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.FAILED);

    rangePolicy = new RowCountRangePolicy(getTestState(20, 8, 0.2), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.FAILED);
  }

  @Test
  public void testRangePolicySuccess() {
    RowCountRangePolicy rangePolicy = new RowCountRangePolicy(getTestState(4, 3, 0.8), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.PASSED);

    rangePolicy = new RowCountRangePolicy(getTestState(20, 12, 0.5), TaskLevelPolicy.Type.FAIL);
    Assert.assertEquals(rangePolicy.executePolicy(), Result.PASSED);
  }

  private State getTestState(long recordsRead, long recordsWritten, double range) {
    State state = new State();
    state.setProp(ConfigurationKeys.EXTRACTOR_ROWS_EXPECTED, recordsRead);
    state.setProp(ConfigurationKeys.WRITER_ROWS_WRITTEN, recordsWritten);
    state.setProp(ConfigurationKeys.ROW_COUNT_RANGE, range);
    return state;
  }
}
