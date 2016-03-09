package gobblin.runtime.locks;

import gobblin.configuration.ConfigurationKeys;
import org.apache.curator.test.TestingServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * Unit test for {@link ZookeeperBasedJobLock}.
 *
 * @author Joel Baranick
 */
@Test(groups = {"gobblin.runtime"})
public class ZookeeperBasedJobLockTest extends JobLockTest {
  private TestingServer testingServer;

  @BeforeClass
  public void setUp() throws Exception {
    testingServer = new TestingServer();
  }

  @Override
  public JobLock getJobLock() throws JobLockException {
    Properties properties = new Properties();
    properties.setProperty(ZookeeperBasedJobLock.CONNECTION_STRING, testingServer.getConnectString());
    properties.setProperty(ConfigurationKeys.JOB_NAME_KEY, "ZookeeperBasedJobLockTest-" + System.currentTimeMillis());
    ZookeeperBasedJobLock lock = new ZookeeperBasedJobLock();
    lock.initialize(properties, new JobLockEventListener());
    return lock;
  }

  @AfterClass
  public void tearDown() throws IOException {
    if (testingServer != null) {
      testingServer.close();
    }
  }
}
