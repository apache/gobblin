package gobblin.config.configstore.impl;

import gobblin.config.utils.PathUtils;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;


public class TestETLHdfsConfigStore {
  private boolean debug = true;

  public void printConfig(Config c, String urn) {
    if (!debug)
      return;
    System.out.println("--------------------------------");
    System.out.println("Config for " + urn);
    for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
      System.out.println("app key: " + entry.getKey() + " ,value:" + entry.getValue());
    }
  }

  private void validateOwnConfig(Config c) {
    // property in datasets/a1/a2/a3/main.conf
    Assert.assertEquals(c.getString("t1.t2.keyInT2"), "valueInT2");
    Assert.assertEquals(c.getInt("foobar.a"), 42);
    Assert.assertEquals(c.getString("propKey1"), "propKey2");
    Assert.assertEquals(c.getString("t1.t2.t3.keyInT3"), "valueInT3");
    Assert.assertEquals(c.getList("listExample").size(), 1);
    Assert.assertEquals(c.getList("listExample").get(0).unwrapped(), "element in a3");
  }

  private void validateImportedConfig(Config c) {
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
  private final String Version = "v3.0";

  @BeforeClass
  public void setUpClass() throws Exception {
    String TestRoot = "HdfsBasedConfigTest";
    File rootDir = Files.createTempDir();
    System.out.println("root dir is " + rootDir);
    File testRootDir = new File(rootDir, TestRoot);

    File input = new File(this.getClass().getResource("/" + TestRoot).getFile());
    FilesUtil.SyncDirs(input, testRootDir);

    URI storeURI = new URI("file://" + testRootDir.getAbsolutePath());
    store = new ETLHdfsConfigStore(storeURI);
    dataset = new URI("datasets/a1/a2/a3");
  }

  @Test
  public void testCurrentVersion() throws Exception {
    Assert.assertEquals(store.getCurrentVersion(), Version);
  }

  @Test(expectedExceptions = gobblin.config.configstore.impl.VersionDoesNotExistException.class)
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
  public void testDatasetResolved() throws Exception {
    Config c = store.getResolvedConfig(dataset, Version);
    Assert.assertEquals(c.entrySet().size(), 9);

    validateOwnConfig(c);
    validateImportedConfig(c);

    Collection<URI> resolvedImport = store.getImportsRecursively(dataset, Version);
    Assert.assertEquals(resolvedImport.size(), 2);
    Iterator<URI> it = resolvedImport.iterator();
    Assert.assertEquals(it.next().toString(), "tags/t1/t2/t3");
    Assert.assertEquals(it.next().toString(), "tags/l1/l2");
  }

  @Test
  public void testDatasetResolvedFromNoExistNode() throws Exception {
    // Node datasets/a1/a2/a3/a4/a5 does not exist
    URI notExist = new URI("datasets/a1/a2/a3/a4/a5");
    Config c = store.getResolvedConfig(notExist, Version);
    Assert.assertEquals(c.entrySet().size(), 9);

    validateOwnConfig(c);
    validateImportedConfig(c);

    Collection<URI> resolvedImport = store.getImportsRecursively(notExist, Version);
    Assert.assertEquals(resolvedImport.size(), 2);
    Iterator<URI> it = resolvedImport.iterator();
    Assert.assertEquals(it.next().toString(), "tags/t1/t2/t3");
    Assert.assertEquals(it.next().toString(), "tags/l1/l2");
  }

  @Test
  public void testImportMapping() throws Exception {
    // own import
    Collection<URI> imported = store.getOwnImports(dataset, Version);
    Assert.assertEquals(imported.size(), 1);
    Iterator<URI> it = imported.iterator();
    Assert.assertEquals(it.next().toString(), "tags/t1/t2/t3");

    // resolved imports
    imported = store.getImportsRecursively(dataset, Version);
    Assert.assertEquals(imported.size(), 2);
    it = imported.iterator();
    Assert.assertEquals(it.next().toString(), "tags/t1/t2/t3");
    Assert.assertEquals(it.next().toString(), "tags/l1/l2");

    // own imported by
    Collection<URI> importedBy = store.getImportedBy(new URI("tags/l1/l2"), Version);
    Set<URI> expected = new HashSet<URI>();
    expected.add(new URI("tags/v1"));
    expected.add(new URI("tags/t1/t2/t3"));

    Assert.assertEquals(importedBy.size(), 2);
    it = importedBy.iterator();
    Assert.assertTrue(expected.contains(it.next()));
    Assert.assertTrue(expected.contains(it.next()));

    // test imported by recursively
    importedBy = store.getImportedByRecursively(new URI("tags/l1/l2"), Version);

    expected.add(new URI("tags/v1/v2/v3"));
    expected.add(new URI("tags/v1/v2"));
    expected.add(new URI("datasets/a1/a2/a3"));

    Assert.assertEquals(importedBy.size(), 5);
    it = importedBy.iterator();
    for (int i = 0; i < importedBy.size(); i++) {
      Assert.assertTrue(expected.contains(it.next()));
    }
  }
}
