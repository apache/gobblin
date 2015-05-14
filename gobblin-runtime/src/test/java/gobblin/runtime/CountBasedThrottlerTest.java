package gobblin.runtime;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link CountBasedThrottler}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime"})
public class CountBasedThrottlerTest {

  @Test
  public void testThrottling() throws InterruptedException {
    Throttler throttler = new CountBasedThrottler(10);
    throttler.start();

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(throttler.waitForNextPermit());
    }
    Assert.assertFalse(throttler.waitForNextPermit());

    throttler.stop();
  }
}
