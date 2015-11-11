package gobblin.config.configstore.impl;

import gobblin.config.configstore.VersionComparator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;


public class TestHdfsConfigStore {

  private boolean debug = true;
  private void printConfig(Config c, String urn) {
    if(!debug) return;
    System.out.println("-----------------------------------");
    System.out.println("Config for " + urn);
    for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
      System.out.println("app key: " + entry.getKey() + " ,value:" + entry.getValue());
    }
  }

  @Test public void testValid() throws Exception {
    VersionComparator<String> vc = new SimpleVersionComparator();
    ETLHdfsConfigStore store = new ETLHdfsConfigStore("ETL_Local", "file:///Users/mitu/HdfsBasedConfigTest", vc);
    Assert.assertEquals(store.getCurrentVersion(), "v3.0");

    String dataset = "datasets/a1/a2/a3";
    String[] expected = {"datasets/a1/a2", "datasets/a1", "datasets", ""};

    List<String> parents = new ArrayList<String>();
    URI parent = store.getParent(new URI(dataset));
    while(parent != null ){
      parents.add(parent.toString());
      parent = store.getParent(parent);
    }

    Assert.assertEquals(parents.size(), expected.length);
    for(int i=0; i<parents.size(); i++){
      Assert.assertEquals(parents.get(i), expected[i]);
    }

    String tags = "tags";
    Collection<URI> children = store.getChildren(new URI(tags));
    Assert.assertEquals(children.size(), 2);
    Iterator<URI> it = children.iterator();

    Assert.assertEquals(it.next().toString(), "tags/l1");
    Assert.assertEquals(it.next().toString(), "tags/t1");

    Config c = store.getOwnConfig(new URI(dataset));
    printConfig(c, dataset);

    Assert.assertEquals(c.entrySet().size(), 5);
    Assert.assertEquals(c.getString("t1.t2.keyInT2"), "valueInT2");
    Assert.assertEquals(c.getInt("foobar.a"), 42);
    Assert.assertEquals(c.getString("propKey1"), "propKey2");
    Assert.assertEquals(c.getString("t1.t2.t3.keyInT3"),"valueInT3");
    Assert.assertEquals(c.getList("listExample").size(), 1);
    Assert.assertEquals(c.getList("listExample").get(0).render(), "\"element in a3\"" );

    Collection<URI> imported = store.getOwnImports(new URI(dataset));
    Assert.assertEquals(imported.size(), 1);
    Assert.assertEquals(imported.iterator().next().toString(), "tags/t1/t2/t3");

    Collection<URI> rootChildren = store.getChildren(new URI(""));
    Assert.assertEquals(rootChildren.size(), 2);
    it = rootChildren.iterator();
    Assert.assertEquals(it.next().toString(), "datasets");
    Assert.assertEquals(it.next().toString(), "tags");

    c = store.getResolvedConfig(new URI(dataset));
    Assert.assertEquals(c.entrySet().size(), 9);

    // property in datasets/a1/a2/a3/main.conf
    Assert.assertEquals(c.getString("t1.t2.keyInT2"), "valueInT2");
    Assert.assertEquals(c.getString("t1.t2.t3.keyInT3"), "valueInT3");
    Assert.assertEquals(c.getString("propKey1"), "propKey2");
    Assert.assertEquals(c.getInt("foobar.a"), 42);
    Assert.assertEquals(c.getList("listExample").size(),1);
    Assert.assertEquals(c.getList("listExample").get(0).unwrapped(), "element in a3");
    
    // property in datasets/a1/a2/main.conf
    Assert.assertEquals(c.getString("keyInT3"), "valueInT3_atA2level");
    Assert.assertEquals(c.getInt("foobar.b"), 43);

    // property in tags/t1/t2/t3/main.conf
    Assert.assertEquals(c.getString("keyInT1_T2_T3"), "valueInT1_T2_T3");

    // property in tags/l1/l2/main.conf 
    Assert.assertEquals(c.getString("keyInL1_L2"), "valueInL1_L2");
    //printConfig(c , "resolved");
    
    Collection<URI> resolvedImport = store.getResolvedImports(new URI(dataset));
    Assert.assertEquals(resolvedImport.size(), 2);
    it = resolvedImport.iterator();
    Assert.assertEquals(it.next().toString(), "tags/t1/t2/t3");
    Assert.assertEquals(it.next().toString(), "tags/l1/l2");

    //////// 
    //    ETLHdfsConfigStore circularStore = new ETLHdfsConfigStore("testSelfImportSelf", "file:///Users/mitu/CircularDependencyTest/selfImportSelf");
    //    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");
    //    System.out.println("circular version "+ circularStore.getCurrentVersion());
    //    
    //    URI circularNode = new URI("tags/t_a_1");
    //    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);

    //    ETLHdfsConfigStore circularStore = new ETLHdfsConfigStore("ancestorImportChild", "file:///Users/mitu/CircularDependencyTest/ancestorImportChild");
    //    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");
    //    
    //    URI circularNode = new URI("tags/t_a_1/t_a_2/t_a_3");
    //    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);

    //    ETLHdfsConfigStore circularStore = new ETLHdfsConfigStore("ancestorImportChild2", "file:///Users/mitu/CircularDependencyTest/ancestorImportChild2");
    //    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");
    //    
    //    URI circularNode = new URI("tags/t_a_1/t_a_2/t_a_3");
    //    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);

    //    ETLHdfsConfigStore circularStore = new ETLHdfsConfigStore("selfImportCircle", "file:///Users/mitu/CircularDependencyTest/selfImportCircle");
    //    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");
    //    
    //    URI circularNode = new URI("tags/t_a_1");
    //    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);

//    ETLHdfsConfigStore circularStore = new ETLHdfsConfigStore("noCircular", "file:///Users/mitu/CircularDependencyTest/noCircular");
//    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");
//
//    URI circularNode = new URI("tags/t_a_1");
//    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);
    
//    ETLHdfsConfigStore circularStore = new ETLHdfsConfigStore("rootImportChild", "file:///Users/mitu/CircularDependencyTest/rootImportChild");
//    Assert.assertEquals(circularStore.getCurrentVersion(), "v1.0");
//
//    URI circularNode = new URI("tags/t_a_1");
//    CircularDependencyChecker.checkCircularDependency(circularStore, circularNode);
  }
}
