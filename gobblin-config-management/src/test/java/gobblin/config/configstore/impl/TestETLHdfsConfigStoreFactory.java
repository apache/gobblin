package gobblin.config.configstore.impl;

import java.io.File;
import java.net.URI;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.Files;

public class TestETLHdfsConfigStoreFactory {
  private ETLHdfsConfigStoreFactory factory;
  private final String Version = "v3.0";
  private File testRootDir;

  @BeforeClass
  public void setUpClass() throws Exception {
    String TestRoot = "HdfsBasedConfigTest";
    File rootDir = Files.createTempDir();
    System.out.println("root dir is " + rootDir);
    testRootDir = new File(rootDir, TestRoot);

    File input = new File(this.getClass().getResource("/" + TestRoot).getFile());
    FilesUtil.SyncDirs(input, testRootDir);
  }
  
  @Test public void testCreation() throws Exception{
    factory = new ETLHdfsConfigStoreFactory(new URI("etl-hdfs://" + testRootDir.getAbsolutePath()));
    ETLHdfsConfigStore cs = factory.getDefaultConfigStore();
    System.out.println("root is " + cs.getStoreURI());
    Assert.assertEquals(cs.getCurrentVersion(), Version);
  }
}
