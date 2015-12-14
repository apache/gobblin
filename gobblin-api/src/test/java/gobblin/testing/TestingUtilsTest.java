package gobblin.testing;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

/** Unit tests for {@link TestingUtils} */
public class TestingUtilsTest {

  @BeforeClass
  public void setUp() {
    BasicConfigurator.configure();
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
  }

  @Test
  public void testComputeRetrySleep() {
    final long infiniteFutureMs = Long.MAX_VALUE;
    Assert.assertEquals(TestingUtils.computeRetrySleep(0, 2, 100, infiniteFutureMs), 1);
    Assert.assertEquals(TestingUtils.computeRetrySleep(10, 2, 100, infiniteFutureMs), 20);
    Assert.assertEquals(TestingUtils.computeRetrySleep(50, 1, 100, infiniteFutureMs), 51);
    Assert.assertEquals(TestingUtils.computeRetrySleep(50, 3, 100, infiniteFutureMs), 100);
    Assert.assertEquals(TestingUtils.computeRetrySleep(50, 3, 60, System.currentTimeMillis() + 100), 60);
    long sleepMs = TestingUtils.computeRetrySleep(50, 3, 1000, System.currentTimeMillis() + 100);
    Assert.assertTrue(sleepMs <= 100);
  }

  @Test
  public void testAssertWithBackoff_conditionTrue() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_conditionTrue");
    TestingUtils.assertWithBackoff(new Callable<Boolean>() {
        @Override public Boolean call() { return true; }
      }, 1000L, "should always succeed", log);
  }

  @Test
  public void testAssertWithBackoff_conditionEventuallyTrue() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_conditionEventuallyTrue");
    //TestingUtils.setLogjLevelForLogger(log, Level.DEBUG);
    final AtomicInteger cnt = new AtomicInteger();
    TestingUtils.assertWithBackoff(new Callable<Boolean>() {
        @Override public Boolean call() { return cnt.incrementAndGet() >= 6; }
      }, 1000L, Optional.of("should eventually succeed"),
      Optional.of(log), Optional.of(2.0), Optional.of(100L));
  }

  @Test
  public void testAssertWithBackoff_conditionFalse() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_conditionFalse");
    //setLogjLevelForLogger(log, Level.DEBUG);
    long startTimeMs = System.currentTimeMillis();
    try {
      TestingUtils.assertWithBackoff(new Callable<Boolean>() {
          @Override public Boolean call() { return false; }
        }, 50L, Optional.of("should timeout"),
        Optional.of(log), Optional.<Double>absent(), Optional.<Long>absent());
      Assert.fail("TimeoutException expected");
    } catch (TimeoutException e) {
      //Expected
    }
    long durationMs = System.currentTimeMillis() - startTimeMs;
    log.debug("assert took " + durationMs + "ms");
    Assert.assertTrue(durationMs >= 50L, Long.toString(durationMs) + ">= 50ms");
  }

  @Test
  public void testAssertWithBackoff_InterruptedException() throws Exception {
    try {
      TestingUtils.assertWithBackoff(new Callable<Boolean>() {
          @Override public Boolean call() throws Exception { throw new InterruptedException(); }
        }, 50L, "should throw InterruptedException");
      Assert.fail("InterruptedException expected");
    } catch (InterruptedException e) {
      //Expected
    }
  }

  @Test
  public void testAssertWithBackoff_RuntimeException() throws Exception {
    Logger log = LoggerFactory.getLogger("testAssertWithBackoff_RuntimeException");
    //setLogjLevelForLogger(log, Level.DEBUG);
    try {
      TestingUtils.assertWithBackoff(new Callable<Boolean>() {
          @Override public Boolean call() throws Exception { throw new Exception("BLAH"); }
        }, 50L, Optional.of("should throw RuntimeException"),
        Optional.of(log), Optional.<Double>absent(), Optional.<Long>absent());
      Assert.fail("should throw RuntimeException");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().indexOf("BLAH") > 0, e.getMessage());
    }
  }

  public static void setLogjLevelForLogger(Logger log, Level logLevel) {
    org.apache.log4j.Logger log4jLogger = org.apache.log4j.Logger.getLogger(log.getName());
    log4jLogger.setLevel(logLevel);
  }

}
