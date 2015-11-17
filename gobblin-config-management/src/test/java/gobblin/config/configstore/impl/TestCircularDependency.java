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
  private final String IncludeFile = "includes";
  private final String Version = "v1.0";
  private File rootDir;
  private File testRootDir;

  @BeforeClass
  public void setUpClass() throws Exception {
    rootDir = Files.createTempDir();
    System.out.println("root dir is " + rootDir);
    testRootDir = new File(rootDir, TestRoot);
  }

  public void testSelfImportCircle() throws Exception {

    String testName = "selfImportCircle";
    File baseDir = new File(testRootDir, testName);
    File versionRootInFile = new File(baseDir, Version);
    String versionRootInResources = "/" + TestRoot + "/" + testName + "/" + Version + "/";

    String a_tagString = "tags/t_a_1";
    File a_tag = new File(versionRootInFile, a_tagString);
    a_tag.mkdirs();

    String b_tagString = "tags/t_b_1";
    File b_tag = new File(versionRootInFile, b_tagString);
    b_tag.mkdirs();

    String c_tagString = "tags/t_c_1";
    File c_tag = new File(versionRootInFile, c_tagString);
    c_tag.mkdirs();

    File t_a_include =
        new File(this.getClass().getResource(versionRootInResources + a_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_a_include, new File(a_tag, IncludeFile));

    File t_b_include =
        new File(this.getClass().getResource(versionRootInResources + b_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_b_include, new File(b_tag, IncludeFile));

    File t_c_include =
        new File(this.getClass().getResource(versionRootInResources + c_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_c_include, new File(c_tag, IncludeFile));

    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    URI circularNode = new URI(a_tagString);
    try {
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("tags/t_a_1 -> tags/t_b_1 -> tags/t_c_1") > 0);
    }
  }

  public void testSelfImportSelf() throws Exception {
    String testName = "selfImportSelf";
    File baseDir = new File(testRootDir, testName);
    File versionRootInFile = new File(baseDir, Version);
    String versionRootInResources = "/" + TestRoot + "/" + testName + "/" + Version + "/";

    String dsString = "datasets/ds1";
    String tagString = "tags/t_a_1";
    File dataset = new File(versionRootInFile, dsString);
    File tag = new File(versionRootInFile, tagString);
    dataset.mkdirs();
    tag.mkdirs();

    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    File ds1_include =
        new File(this.getClass().getResource(versionRootInResources + dsString + "/" + IncludeFile).getFile());
    Files.copy(ds1_include, new File(dataset, IncludeFile));

    File t_a_1_include =
        new File(this.getClass().getResource(versionRootInResources + tagString + "/" + IncludeFile).getFile());
    Files.copy(t_a_1_include, new File(tag, IncludeFile));

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    URI circularNode = new URI(tagString);
    try {
      CircularDependencyChecker.checkCircularDependency(circularStore, Version, circularNode);
      Assert.fail("Did not catch expected CircularDependencyException");
    } catch (CircularDependencyException e) {
      Assert.assertTrue(e.getMessage().indexOf("import self") > 0);
    }
  }

  public void testAncestorImportChild() throws Exception {

    String testName = "ancestorImportChild";
    File baseDir = new File(testRootDir, testName);
    File versionRootInFile = new File(baseDir, Version);
    String versionRootInResources = "/" + TestRoot + "/" + testName + "/" + Version + "/";

    String tagString = "tags/t_a_1/t_a_2/t_a_3";
    File tag = new File(versionRootInFile, tagString);
    tag.mkdirs();
    tagString = "tags/t_a_1";
    tag = new File(versionRootInFile, tagString);

    File t_a_1_include =
        new File(this.getClass().getResource(versionRootInResources + tagString + "/" + IncludeFile).getFile());
    Files.copy(t_a_1_include, new File(tag, IncludeFile));

    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

    tagString = "tags/t_a_1/t_a_2/t_a_3";
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
    File versionRootInFile = new File(baseDir, Version);
    String versionRootInResources = "/" + TestRoot + "/" + testName + "/" + Version + "/";

    String a_tagString = "tags/t_a_1";
    File a_tag = new File(versionRootInFile, a_tagString);
    a_tag.mkdirs();

    String b_tagString = "tags/t_b_1/t_b_2";
    File b_tag = new File(versionRootInFile, b_tagString);
    b_tag.mkdirs();

    File t_a_include =
        new File(this.getClass().getResource(versionRootInResources + a_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_a_include, new File(a_tag, IncludeFile));

    File t_b_include =
        new File(this.getClass().getResource(versionRootInResources + b_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_b_include, new File(b_tag, IncludeFile));

    a_tagString = "tags/t_a_1/t_a_2/t_a_3";
    a_tag = new File(versionRootInFile, a_tagString);
    a_tag.mkdirs();

    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI);
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
    File versionRootInFile = new File(baseDir, Version);
    String versionRootInResources = "/" + TestRoot + "/" + testName + "/" + Version + "/";

    String tagString = "tags/t_a_1/t_a_2/t_a_3";
    File tag = new File(versionRootInFile, tagString);
    tag.mkdirs();

    File rootDir = new File(testRootDir, testName + "/v1.0");

    File root_include = new File(this.getClass().getResource(versionRootInResources + IncludeFile).getFile());
    Files.copy(root_include, new File(rootDir, IncludeFile));

    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

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
    File versionRootInFile = new File(baseDir, Version);
    String versionRootInResources = "/" + TestRoot + "/" + testName + "/" + Version + "/";

    String a_tagString = "tags/t_a_1/t_a_2/t_a_3";
    File a_tag = new File(versionRootInFile, a_tagString);
    a_tag.mkdirs();

    String b_tagString = "tags/t_b_1/t_b_2";
    File b_tag = new File(versionRootInFile, b_tagString);
    b_tag.mkdirs();

    File t_a_include =
        new File(this.getClass().getResource(versionRootInResources + a_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_a_include, new File(a_tag, IncludeFile));

    File t_b_include =
        new File(this.getClass().getResource(versionRootInResources + b_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_b_include, new File(b_tag, IncludeFile));

    URI storeURI = new URI("file://" + baseDir.getAbsolutePath());
    //System.out.println("store uri is " + storeURI);

    HdfsConfigStoreWithOwnInclude circularStore = new HdfsConfigStoreWithOwnInclude(storeURI);
    Assert.assertEquals(circularStore.getCurrentVersion(), Version);

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
