package gobblin.config.configstore.impl;

import gobblin.config.utils.CircularDependencyChecker;
import gobblin.config.utils.CircularDependencyException;

import java.io.File;
import java.net.URI;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;


@Test(groups = { "gobblin.config.configstore.impl" })
public class TestCircularDependency {

  private final String TestRoot = "CircularDependencyTest";
  private final String Version = "v1.0";
  private File rootDir;
  private File testRootDir;

  @BeforeClass
  public void setUpClass() throws Exception {
    rootDir = Files.createTempDir();
    System.out.println("root dir is " + rootDir);
    testRootDir = new File(rootDir, TestRoot);

    File input = new File(this.getClass().getResource("/" + TestRoot).getFile());
    FilesUtil.SyncDirs(input, testRootDir);
  }
  
  @Test public void testSelfImportCircle() throws Exception {

    String testName = "selfImportCircle";
    File baseDir = new File(testRootDir, testName);
    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI, storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    String a_tagString = "tags/t_a_1";
    URI circularNode = new URI(a_tagString);
    try {
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("tags/t_a_1 -> tags/t_b_1 -> tags/t_c_1") > 0);
    }
  }

  @Test public void testSelfImportSelf() throws Exception {
    String testName = "selfImportSelf";
    File baseDir = new File(testRootDir, testName);

    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI, storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    String tagString = "tags/t_a_1";
    URI circularNode = new URI(tagString);
    try {
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("import self") > 0);
    }
  }
  

  @Test public void testAncestorImportChild() throws Exception {

    String testName = "ancestorImportChild";
    File baseDir = new File(testRootDir, testName);
    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI, storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    String tagString = "tags/t_a_1/t_a_2/t_a_3";
    URI circularNode = new URI(tagString);

    try {
      // check from children
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("tags/t_a_1/t_a_2/t_a_3 -> tags/t_a_1/t_a_2 -> tags/t_a_1") > 0);
    }

    tagString = "tags/t_a_1";
    circularNode = new URI(tagString);

    try {
      // check from parent
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage()
          .indexOf("tags/t_a_1 -> tags/t_a_1/t_a_2/t_a_3 -> tags/t_a_1/t_a_2 -> tags/t_a_1") > 0);
    }
  }

  public void testAncestorImportChild2() throws Exception {
    String testName = "ancestorImportChild2";
    File baseDir = new File(testRootDir, testName);
    String a_tagString = "tags/t_a_1/t_a_2/t_a_3";
    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI,storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    URI circularNode = new URI(a_tagString);
    try {
      // check from children
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf(
          "tags/t_a_1/t_a_2/t_a_3 -> tags/t_a_1/t_a_2 -> tags/t_a_1 -> tags/t_b_1/t_b_2") > 0);
    }

    circularNode = new URI("tags/t_a_1");
    try {
      // check from parent
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf(
          "tags/t_a_1 -> tags/t_b_1/t_b_2 -> tags/t_a_1/t_a_2/t_a_3 -> tags/t_a_1/t_a_2 -> tags/t_a_1") > 0);
    }
  }

  public void testRootImportChild() throws Exception {
    String testName = "rootImportChild";
    File baseDir = new File(testRootDir, testName);
    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI,storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    String tagString = "tags/t_a_1/t_a_2/t_a_3";
    URI circularNode = new URI(tagString);
    try {
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("tags/t_a_1/t_a_2/t_a_3 -> tags/t_a_1/t_a_2 -> tags/t_a_1") > 0);
    }
  }

  public void testNoCircular() throws Exception {
    String testName = "noCircular";
    File baseDir = new File(testRootDir, testName);
    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI,storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    String a_tagString = "tags/t_a_1/t_a_2/t_a_3";
    URI circularNode = new URI(a_tagString);
    CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
  }

  @AfterClass
  public void tearDownClass() throws Exception {
    if (rootDir != null) {
      FileUtils.deleteDirectory(rootDir);
    }
  }
}
