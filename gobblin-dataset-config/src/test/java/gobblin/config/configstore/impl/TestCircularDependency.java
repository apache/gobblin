package gobblin.config.configstore.impl;

import java.net.URI;

import gobblin.config.configstore.VersionComparator;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestCircularDependency {

  @Test(expectedExceptions = CircularDependencyException.class)
  public void testSelfImportSelf() throws Exception {
    ETLHdfsConfigStore circularStore =
        new ETLHdfsConfigStore("testSelfImportSelf", "file:///Users/mitu/CircularDependencyTest/selfImportSelf");
    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");

    URI circularNode = new URI("tags/t_a_1");
    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);
  }

  @Test(expectedExceptions = CircularDependencyException.class)
  public void testAncestorImportChild() throws Exception {
    ETLHdfsConfigStore circularStore =
        new ETLHdfsConfigStore("ancestorImportChild", "file:///Users/mitu/CircularDependencyTest/ancestorImportChild");
    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");

    URI circularNode = new URI("tags/t_a_1/t_a_2/t_a_3");
    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);

  }

  @Test(expectedExceptions = CircularDependencyException.class)
  public void testAncestorImportChild2() throws Exception {
    ETLHdfsConfigStore circularStore =
        new ETLHdfsConfigStore("ancestorImportChild2", "file:///Users/mitu/CircularDependencyTest/ancestorImportChild2");
    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");

    URI circularNode = new URI("tags/t_a_1/t_a_2/t_a_3");
    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);
  }

  @Test(expectedExceptions = CircularDependencyException.class)
  public void testSelfImportCircle() throws Exception {
    ETLHdfsConfigStore circularStore =
        new ETLHdfsConfigStore("selfImportCircle", "file:///Users/mitu/CircularDependencyTest/selfImportCircle");
    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");

    URI circularNode = new URI("tags/t_a_1");
    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);
  }

  @Test(expectedExceptions = CircularDependencyException.class)
  public void testRootImportChild() throws Exception {
    ETLHdfsConfigStore circularStore =
        new ETLHdfsConfigStore("rootImportChild", "file:///Users/mitu/CircularDependencyTest/rootImportChild");
    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");

    URI circularNode = new URI("tags/t_a_1");
    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);
  }

  @Test
  public void testNoCircular() throws Exception {
    ETLHdfsConfigStore circularStore =
        new ETLHdfsConfigStore("noCircular", "file:///Users/mitu/CircularDependencyTest/noCircular");
    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");

    URI circularNode = new URI("tags/t_a_1");
    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);
  }
}
