package gobblin.config.configstore.impl;

import java.io.File;
import java.net.URI;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

public class TestETLHdfsConfigStore {
  private boolean debug = true;
  public void printConfig(Config c, String urn) {
    if(!debug) return;
    System.out.println("--------------------------------");
    System.out.println("Config for " + urn);
    for (Map.Entry<String, ConfigValue> entry : c.entrySet()) {
      System.out.println("app key: " + entry.getKey() + " ,value:" + entry.getValue());
    }
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
  
  private ETLHdfsConfigStore store;
  private URI dataset ;
  private final String Version = "v3.0";

  @BeforeClass
  public void setUpClass() throws Exception {
    String TestRoot = "HdfsBasedConfigTest";
    String IncludeFile = HdfsConfigStoreWithOwnInclude.INCLUDE_FILE_NAME;
    String MainFile = BaseHdfsConfigStore.CONFIG_FILE_NAME;
    
    File rootDir = Files.createTempDir();
    System.out.println("root dir is " + rootDir);
    File testRootDir = new File(rootDir, TestRoot);
    
    File versionRootInFile = new File(testRootDir, Version);
    String versionRootInResources = "/" + TestRoot + "/" + Version + "/";

    String l_tagString = "tags/l1/l2";
    File l_tag = new File(versionRootInFile, l_tagString);
    l_tag.mkdirs();
    File l_tag_main =
        new File(this.getClass().getResource(versionRootInResources + l_tagString + "/" + MainFile).getFile());
    Files.copy(l_tag_main, new File(l_tag, MainFile));
    
    String t_tagString = "tags/t1/t2/t3";
    File t_tag = new File(versionRootInFile, t_tagString);
    t_tag.mkdirs();
    
    File t_tag_main =
        new File(this.getClass().getResource(versionRootInResources + t_tagString + "/" + MainFile).getFile());
    Files.copy(t_tag_main, new File(t_tag, MainFile));
    
    File t_tag_include =
        new File(this.getClass().getResource(versionRootInResources + t_tagString + "/" + IncludeFile).getFile());
    Files.copy(t_tag_include, new File(t_tag, IncludeFile));
    
    String v_tagString = "tags/v1/v2/v3";
    File v_tag = new File(versionRootInFile, v_tagString);
    v_tag.mkdirs();
    
    v_tagString = "tags/v1";
    v_tag = new File(versionRootInFile, v_tagString);
    File v_tag_include =
        new File(this.getClass().getResource(versionRootInResources + v_tagString + "/" + IncludeFile).getFile());
    Files.copy(v_tag_include, new File(v_tag, IncludeFile));
    
    String ds_tagString = "datasets/a1/a2/a3";
    File ds_tag = new File(versionRootInFile, ds_tagString);
    ds_tag.mkdirs();
    
    File ds_main =
        new File(this.getClass().getResource(versionRootInResources + ds_tagString + "/" + MainFile).getFile());
    Files.copy(ds_main, new File(ds_tag, MainFile));
    
    File ds_include =
        new File(this.getClass().getResource(versionRootInResources + ds_tagString + "/" + IncludeFile).getFile());
    Files.copy(ds_include, new File(ds_tag, IncludeFile));
    
    ds_tagString = "datasets/a1/a2";
    ds_tag = new File(versionRootInFile, ds_tagString);
    ds_main =
        new File(this.getClass().getResource(versionRootInResources + ds_tagString + "/" + MainFile).getFile());
    Files.copy(ds_main, new File(ds_tag, MainFile));

    URI storeURI = new URI("file://" + testRootDir.getAbsolutePath());
    store = new ETLHdfsConfigStore(storeURI);
  }
  
  @Test public void testCurrentVersion() throws Exception {
    Assert.assertEquals(store.getCurrentVersion(), Version);
  }
  
  @Test (expectedExceptions = gobblin.config.configstore.impl.VersionDoesNotExistException.class)
  public void testInvalidVersion() throws Exception{
    store.getChildren(new URI(""), "V1.0");
  }
}
