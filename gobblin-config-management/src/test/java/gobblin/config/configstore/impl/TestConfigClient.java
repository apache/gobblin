package gobblin.config.configstore.impl;


import gobblin.config.client.ConfigClient;

import java.io.File;
import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;


@Test(groups = { "gobblin.config.client" })

public class TestConfigClient {
  
  private File rootDir;
  private File testRootDir;
  private ConfigClient cc;
  private URI dsURI;
  
  @BeforeClass
  public void setUpClass() throws Exception {
    String TestRoot = "HdfsBasedConfigTest";
    rootDir = Files.createTempDir();
    System.out.println("root dir is " + rootDir);
    testRootDir = new File(rootDir, TestRoot);

    File input = new File(this.getClass().getResource("/" + TestRoot).getFile());
    FilesUtil.SyncDirs(input, testRootDir);
    cc = ConfigClient.createDefaultConfigClient();
    dsURI = new URI("etl-hdfs://" + testRootDir.getAbsolutePath() + "/datasets/a1/a2/a3");
  }
  
  @Test
  public void testDataset() throws Exception{
    Config c = cc.getConfig(dsURI);
    //TestETLHdfsConfigStore.printConfig(c, dsURI.toString());
    TestETLHdfsConfigStore.validateImportedConfig(c);
  }
  
  @Test
  public void testDatasetImports() throws Exception{
    Collection<URI> imports = cc.getImports(dsURI, false);
    for(URI i: imports){
      System.out.println("direct imports is " +i);
      // test the reverse mapping
      testDatasetImportedBy(dsURI, i, false);
    }
    
    imports = cc.getImports(dsURI, true);
    for(URI i: imports){
      System.out.println("resolved imports is " +i);
      // test the reverse mapping
      testDatasetImportedBy(dsURI, i, true);
    }
  }
  
  protected void testDatasetImportedBy(URI source, URI sourceImports, boolean recursively) throws Exception{
    Collection<URI> imported = cc.getImportedBy(sourceImports, recursively);
    boolean found = false;
    for(URI i: imported){
      if(i.equals(source)){
        found = true;
      }
    }
    Assert.assertTrue(found);
  }
}
