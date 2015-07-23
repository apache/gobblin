package gobblin.runtime;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link CountBasedLimiter}.
 *
 * @author ynli
 */
@Test(groups = {"gobblin.runtime"})
public class CountBasedLimiterTest {

  @Test
  public void testThrottling() throws InterruptedException {
    Limiter limiter = new CountBasedLimiter(10);
    limiter.start();

    for (int i = 0; i < 10; i++) {
      Assert.assertTrue(limiter.acquirePermits(1) != null);
    }
    Assert.assertTrue(limiter.acquirePermits(1) == null);

    limiter.stop();
  }
}
