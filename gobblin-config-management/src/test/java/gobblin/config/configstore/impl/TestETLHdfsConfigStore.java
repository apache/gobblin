package gobblin.config.configstore.impl;

import gobblin.config.utils.PathUtils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;


public class TestETLHdfsConfigStore {

  public static void printConfig(Config c, String urn) {
    System.out.println("--------------------------------");
    System.out.println("Config for " + urn);
    for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
      System.out.println("app key: " + entry.getKey() + " ,value:" + entry.getValue());
    }
  }

  protected static void validateOwnConfig(Config c) {
    // property in datasets/a1/a2/a3/main.conf
    Assert.assertEquals(c.getString("t1.t2.keyInT2"), "valueInT2");
    Assert.assertEquals(c.getInt("foobar.a"), 42);
    Assert.assertEquals(c.getString("propKey1"), "propKey2");
    Assert.assertEquals(c.getString("t1.t2.t3.keyInT3"), "valueInT3");
    Assert.assertEquals(c.getList("listExample").size(), 1);
    Assert.assertEquals(c.getList("listExample").get(0).unwrapped(), "element in a3");
  }

  protected static void validateImportedConfig(Config c) {
    // property in datasets/a1/a2/main.conf
    Assert.assertEquals(c.getString("keyInT3"), "valueInT3_atA2level");
    Assert.assertEquals(c.getInt("foobar.b"), 43);

    // property in tags/t1/t2/t3/main.conf
    Assert.assertEquals(c.getString("keyInT1_T2_T3"), "valueInT1_T2_T3");

    // property in tags/l1/l2/main.conf 
    Assert.assertEquals(c.getString("keyInL1_L2"), "valueInL1_L2");
  }

  private ETLHdfsConfigStore store;
  private URI dataset;
  private File rootDir;
  private final String Version = "v3.0";

  @BeforeClass
  public void setUpClass() throws Exception {
    String TestRoot = "HdfsBasedConfigTest";
    rootDir = Files.createTempDir();
    System.out.println("root dir is " + rootDir);
    File testRootDir = new File(rootDir, TestRoot);

    File input = new File(this.getClass().getResource("/" + TestRoot).getFile());
    FilesUtil.SyncDirs(input, testRootDir);

    URI storeURI = new URI("file://" + testRootDir.getAbsolutePath());
    store = new ETLHdfsConfigStore(storeURI, storeURI);
    dataset = new URI("datasets/a1/a2/a3");
  }

  @Test
  public void testCurrentVersion() throws Exception {
    Assert.assertEquals(store.getCurrentVersion(), Version);
  }

  @Test(expectedExceptions = gobblin.config.configstore.VersionDoesNotExistException.class)
  public void testInvalidVersion() throws Exception {
    store.getChildren(new URI(""), "V1.0");
  }

  @Test
  public void testGetChildren() throws Exception {
    // test root children
    Collection<URI> rootChildren = store.getChildren(new URI(""), Version);
    Assert.assertEquals(rootChildren.size(), 2);
    Iterator<URI> it = rootChildren.iterator();
    Assert.assertEquals(it.next().toString(), "datasets");
    Assert.assertEquals(it.next().toString(), "tags");

    // test tags children
    String tags = "tags";
    Collection<URI> children = store.getChildren(new URI(tags), Version);
    Assert.assertEquals(children.size(), 3);
    it = children.iterator();
    Assert.assertEquals(it.next().toString(), "tags/l1");
    Assert.assertEquals(it.next().toString(), "tags/t1");
    Assert.assertEquals(it.next().toString(), "tags/v1");
  }

  @Test
  public void testDatasetBasics() throws Exception {
    // test dataset "datasets/a1/a2/a3"
    String[] expected = { "datasets/a1/a2", "datasets/a1", "datasets", "" };

    List<String> parents = new ArrayList<String>();
    URI parent = PathUtils.getParentURI(dataset);
    while (parent != null) {
      parents.add(parent.toString());
      parent = PathUtils.getParentURI(parent);
    }

    Assert.assertEquals(parents.size(), expected.length);
    for (int i = 0; i < parents.size(); i++) {
      Assert.assertEquals(parents.get(i), expected[i]);
    }

    Config c = store.getOwnConfig(dataset, Version);
    //printConfig(c, dataset);
    validateOwnConfig(c);

    Collection<URI> imported = store.getOwnImports(dataset, Version);
    Assert.assertEquals(imported.size(), 1);
    Assert.assertEquals(imported.iterator().next().toString(), "tags/t1/t2/t3");
  }

  @Test
  public void testImportMapping() throws Exception {
    // own import
    Collection<URI> imported = store.getOwnImports(dataset, Version);
    Assert.assertEquals(imported.size(), 1);
    Iterator<URI> it = imported.iterator();
    Assert.assertEquals(it.next().toString(), "tags/t1/t2/t3");
  }

  @AfterClass
  public void tearDownClass() throws Exception {
    if (rootDir != null) {
      FileUtils.deleteDirectory(rootDir);
    }
  }
}
