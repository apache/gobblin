package gobblin.config.configstore.impl;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;

import gobblin.config.configstore.VersionComparator;

import org.testng.Assert;
import org.testng.annotations.Test;


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
    HdfsConfigStoreWithOwnInclude store = new HdfsConfigStoreWithOwnInclude("ETL_Local", "file:///Users/mitu/HdfsBasedConfigTest", vc);
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
    
    Collection<URI> imported = store.getImports(new URI(dataset));
    Assert.assertEquals(imported.size(), 1);
    Assert.assertEquals(imported.iterator().next().toString(), "tags/t1/t2/t3");

  }
}
