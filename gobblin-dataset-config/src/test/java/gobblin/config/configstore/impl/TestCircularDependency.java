package gobblin.config.configstore.impl;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCircularDependency {
  
  @Test public void testSelfImportSelf() throws Exception {
    ETLHdfsConfigStore store = new ETLHdfsConfigStore("testSelfImportSelf", "file:///Users/mitu/CircularDependencyTest/selfImportSelf");
    Assert.assertEquals(store.getCurrentVersion(), "v1.0");
  }
}
