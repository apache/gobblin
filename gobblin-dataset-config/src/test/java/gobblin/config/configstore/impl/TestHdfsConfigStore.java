package gobblin.config.configstore.impl;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.impl.SimpleConfigClient;
import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.VersionComparator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;


public class TestHdfsConfigStore {

  private boolean debug = true;
  public void printConfig(Config c, String urn) {
    if(!debug) return;
    System.out.println("--------------------------------");
    System.out.println("Config for " + urn);
    for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
      System.out.println("app key: " + entry.getKey() + " ,value:" + entry.getValue());
    }
  }
  
  private ETLHdfsConfigStore store;
  private String dataset = "datasets/a1/a2/a3";
  
  @BeforeClass
  public void setUp(){
    VersionComparator<String> vc = new SimpleVersionComparator();
    store = new ETLHdfsConfigStore("ETL_Local", "file:///Users/mitu/HdfsBasedConfigTest", vc);
    Assert.assertEquals(store.getCurrentVersion(), "v3.0");
  }
  
  private void validateOwnConfig(Config c){
    // property in datasets/a1/a2/a3/main.conf
    Assert.assertEquals(c.getString("t1.t2.keyInT2"), "valueInT2");
    Assert.assertEquals(c.getInt("foobar.a"), 42);
    Assert.assertEquals(c.getString("propKey1"), "propKey2");
    Assert.assertEquals(c.getString("t1.t2.t3.keyInT3"),"valueInT3");
    Assert.assertEquals(c.getList("listExample").size(), 1);
    Assert.assertEquals(c.getList("listExample").get(0).unwrapped(), "element in a3" );
  }
  
  private void validateOtherConfig(Config c){
    // property in datasets/a1/a2/main.conf
    Assert.assertEquals(c.getString("keyInT3"), "valueInT3_atA2level");
    Assert.assertEquals(c.getInt("foobar.b"), 43);

    // property in tags/t1/t2/t3/main.conf
    Assert.assertEquals(c.getString("keyInT1_T2_T3"), "valueInT1_T2_T3");

    // property in tags/l1/l2/main.conf 
    Assert.assertEquals(c.getString("keyInL1_L2"), "valueInL1_L2");
  }

  @Test public void testGetChildren() throws Exception {
    // test root children
    Collection<URI> rootChildren = store.getChildren(new URI(""));
    Assert.assertEquals(rootChildren.size(), 2);
    Iterator<URI> it = rootChildren.iterator();
    Assert.assertEquals(it.next().toString(), "datasets");
    Assert.assertEquals(it.next().toString(), "tags");
    
    // test tags children
    String tags = "tags";
    Collection<URI> children = store.getChildren(new URI(tags));
    Assert.assertEquals(children.size(), 2);
    it = children.iterator();
    Assert.assertEquals(it.next().toString(), "tags/l1");
    Assert.assertEquals(it.next().toString(), "tags/t1");
  }
  
  @Test public void testDataset() throws Exception{
    // test dataset "datasets/a1/a2/a3"
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

    Config c = store.getOwnConfig(new URI(dataset));
    //printConfig(c, dataset);
    validateOwnConfig(c);
  }
  
  @Test public void testDatasetResolved() throws Exception{
    Collection<URI> imported = store.getOwnImports(new URI(dataset));
    Assert.assertEquals(imported.size(), 1);
    Assert.assertEquals(imported.iterator().next().toString(), "tags/t1/t2/t3");

    Config c = store.getResolvedConfig(new URI(dataset));
    Assert.assertEquals(c.entrySet().size(), 9);
    
    validateOwnConfig(c);
    validateOtherConfig(c);
    
    Collection<URI> resolvedImport = store.getResolvedImports(new URI(dataset));
    Assert.assertEquals(resolvedImport.size(), 2);
    Iterator<URI> it = resolvedImport.iterator();
    Assert.assertEquals(it.next().toString(), "tags/t1/t2/t3");
    Assert.assertEquals(it.next().toString(), "tags/l1/l2");
  }
  
  @Test public void testDatasetFromConfigStoreFactory() throws Exception {
    ConfigStoreFactoryUsingProperty csf = new ConfigStoreFactoryUsingProperty("/Users/mitu/CircularDependencyTest/stores.conf");
    Collection<String> schemes = csf.getConfigStoreSchemes();
    ConfigStore cs1 = csf.getConfigStore(schemes.iterator().next());
    ConfigClient client = new SimpleConfigClient();
    Config c = client.getConfig(cs1, new URI(dataset));
    //printConfig(c , "from config client ");
    validateOwnConfig(c);
    validateOtherConfig(c);

  }
}
