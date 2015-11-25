package gobblin.config.configstore.impl;


import gobblin.config.client.ConfigClient;

import java.io.File;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.typesafe.config.Config;


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
    Assert.assertTrue(imports.size()==1);
    for(URI i: imports){
      System.out.println("direct imports is " +i);
      Assert.assertTrue(i.toString().endsWith("/HdfsBasedConfigTest/tags/t1/t2/t3"));
      // test the reverse mapping
      testDatasetImportedBy(dsURI, i, false);
    }
    
    imports = cc.getImports(dsURI, true);
    Assert.assertTrue(imports.size()==2);
    for(URI i: imports){
      System.out.println("resolved imports is " +i);
      Assert.assertTrue(i.toString().endsWith("/HdfsBasedConfigTest/tags/t1/t2/t3") || 
          i.toString().endsWith("/HdfsBasedConfigTest/tags/l1/l2"));
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
  
  @Test
  public void testImportMapping() throws Exception {
    URI tagURI = new URI("etl-hdfs://" + testRootDir.getAbsolutePath() + "/tags/l1/l2");
    // own imported by
    Collection<URI> importedBy = cc.getImportedBy(tagURI, false);

    Assert.assertEquals(importedBy.size(), 2);
    for(URI i: importedBy){
      System.out.println("direct imported by  is " +i);
      Assert.assertTrue(i.toString().endsWith("/HdfsBasedConfigTest/tags/t1/t2/t3")
          || i.toString().endsWith("/HdfsBasedConfigTest/tags/v1"));
    }

    importedBy = cc.getImportedBy(tagURI, true);

    Assert.assertEquals(importedBy.size(), 5);
    for(URI i: importedBy){
      System.out.println("recursively imported by  is " +i);
      Assert.assertTrue(i.toString().endsWith("/HdfsBasedConfigTest/tags/v1/v2/v3")
          || i.toString().endsWith("/HdfsBasedConfigTest/tags/v1") 
          || i.toString().endsWith("/HdfsBasedConfigTest/datasets/a1/a2/a3") 
          || i.toString().endsWith("/HdfsBasedConfigTest/tags/t1/t2/t3") 
          || i.toString().endsWith("/HdfsBasedConfigTest/tags/v1/v2") );
    }
  }
  
  @Test
  public void testDatasetResolvedFromNoExistNode() throws Exception {
    // Node datasets/a1/a2/a3/a4/a5 does not exist
    URI notExist = new URI("etl-hdfs://" + testRootDir.getAbsolutePath() + "/datasets/a1/a2/a3/a4/a5");
    Config c = cc.getConfig(notExist);
    TestETLHdfsConfigStore.validateImportedConfig(c);
  }
  
  @AfterClass
  public void tearDownClass() throws Exception {
    if (rootDir != null) {
      FileUtils.deleteDirectory(rootDir);
    }
  }
}
