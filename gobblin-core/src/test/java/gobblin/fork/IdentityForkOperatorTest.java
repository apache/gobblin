package gobblin.fork;

import gobblin.configuration.ConfigurationKeys;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.configuration.WorkUnitState;


/**
 * Unit tests for {@link IdentityForkOperator}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.fork"})
public class IdentityForkOperatorTest {

  @Test
  public void testForkMethods() {
    ForkOperator<String, String> dummyForkOperator = new IdentityForkOperator<String, String>();
    WorkUnitState workUnitState = new WorkUnitState();

    workUnitState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, 2);
    List<Boolean> schemas = dummyForkOperator.forkSchema(workUnitState, "");
    Assert.assertEquals(schemas, Arrays.asList(true, true));
    List<Boolean> records = dummyForkOperator.forkDataRecord(workUnitState, "");
    Assert.assertEquals(records, Arrays.asList(true, true));
    Assert.assertEquals(dummyForkOperator.getBranches(workUnitState), 2);

    workUnitState.setProp(ConfigurationKeys.FORK_BRANCHES_KEY, 0);
    schemas = dummyForkOperator.forkSchema(workUnitState, "");
    Assert.assertTrue(schemas.isEmpty());
    records = dummyForkOperator.forkDataRecord(workUnitState, "");
    Assert.assertTrue(records.isEmpty());
    Assert.assertEquals(dummyForkOperator.getBranches(workUnitState), 0);
  }
}
