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

  @Test
  public void testCreation() throws Exception {
    factory = new ETLHdfsConfigStoreFactory(new URI("etl-hdfs://" + testRootDir.getAbsolutePath()));
    ETLHdfsConfigStore cs = factory.getDefaultConfigStore();
    Assert.assertTrue(cs.getStoreURI().toString().indexOf(testRootDir.toString()) > 0);
    Assert.assertEquals(cs.getCurrentVersion(), Version);

    ETLHdfsConfigStore cs2 =
        factory.createConfigStore(new URI("etl-hdfs://" + testRootDir.getAbsolutePath() + "/v3.0/datasets/a1/a2"));
    Assert.assertTrue(cs2.getStoreURI().toString().indexOf(testRootDir.toString()) > 0);
    Assert.assertEquals(cs2.getCurrentVersion(), Version);
  }

  @Test(expectedExceptions = gobblin.config.configstore.impl.ConfigStoreCreationException.class)
  public void testWrongCreation() throws Exception {
    factory = new ETLHdfsConfigStoreFactory(new URI("foo-hdfs://" + testRootDir.getAbsolutePath()));
  }

  @Test(expectedExceptions = gobblin.config.configstore.impl.ConfigStoreCreationException.class)
  public void testWrongCreation2() throws Exception {
    factory = new ETLHdfsConfigStoreFactory(new URI("etl-hdfs://" + testRootDir.getAbsolutePath()));
    factory.createConfigStore(new URI("foo-hdfs://" + testRootDir.getAbsolutePath() + "/v3.0/datasets/a1/a2"));
  }
  
  @Test (expectedExceptions = gobblin.config.configstore.impl.ConfigStoreCreationException.class)
  public void testWrongHdfsConfigStore() throws Exception {   
    String WrongTestRoot = "WrongHdfsConfigStore";
    File rootDir = Files.createTempDir();
    System.out.println("wrong root dir is " + rootDir);
    File WrongTestRootDir = new File(rootDir, WrongTestRoot);
    File input = new File(this.getClass().getResource("/" + WrongTestRoot).getFile());
    FilesUtil.SyncDirs(input, WrongTestRootDir);
    
    factory = new ETLHdfsConfigStoreFactory(new URI("etl-hdfs://" + WrongTestRootDir.getAbsolutePath()));
  }
  
}
