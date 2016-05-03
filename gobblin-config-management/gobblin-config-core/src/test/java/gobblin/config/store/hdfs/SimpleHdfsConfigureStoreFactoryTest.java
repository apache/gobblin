package gobblin.config.store.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import gobblin.config.store.api.ConfigStoreCreationException;


/**
 * Unit tests for {@link SimpleHDFSConfigStoreFactory}.
 */
@Test(groups = "gobblin.config.store.hdfs")
public class SimpleHdfsConfigureStoreFactoryTest {

  @Test
  public void testGetDefaults() throws URISyntaxException, ConfigStoreCreationException, IOException {
    Path configStoreDir = new Path(SimpleHDFSConfigStore.CONFIG_STORE_NAME);
    FileSystem fs = null;

    try {
      fs = FileSystem.getLocal(new Configuration());
      fs.mkdirs(configStoreDir);

      SimpleLocalHDFSConfigStoreFactory simpleLocalHDFSConfigStoreFactory = new SimpleLocalHDFSConfigStoreFactory();

      URI configKey = new URI(simpleLocalHDFSConfigStoreFactory.getScheme(), "", "", "", "");
      SimpleHDFSConfigStore simpleHDFSConfigStore = simpleLocalHDFSConfigStoreFactory.createConfigStore(configKey);

      Assert
          .assertEquals(simpleHDFSConfigStore.getStoreURI().getScheme(), simpleLocalHDFSConfigStoreFactory.getScheme());
      Assert.assertNull(simpleHDFSConfigStore.getStoreURI().getAuthority());
      Assert.assertEquals(simpleHDFSConfigStore.getStoreURI().getPath(), System.getProperty("user.dir"));
    } finally {
      if (fs != null && fs.exists(configStoreDir)) {
        fs.delete(configStoreDir, true);
      }
    }
  }


  @Test
  public void testConfiguration() throws Exception {
    FileSystem localFS = FileSystem.getLocal(new Configuration());
    Path testRoot = localFS.makeQualified(new Path("dir1"));
    Path configRoot = localFS.makeQualified(new Path(testRoot, "dir2"));
    Path configStoreRoot = new Path(configRoot,
                                    SimpleHDFSConfigStore.CONFIG_STORE_NAME);
    Assert.assertTrue(localFS.mkdirs(configStoreRoot));
    try {
      Config confConf1 =
          ConfigFactory.empty().withValue(SimpleHDFSConfigStoreFactory.DEFAULT_STORE_URI_KEY,
                                          ConfigValueFactory.fromAnyRef(configRoot.toString()));
      SimpleHDFSConfigStoreFactory confFactory = new SimpleHDFSConfigStoreFactory(confConf1);
      Assert.assertTrue(confFactory.hasDefaultStoreURI());
      Assert.assertEquals(confFactory.getDefaultStoreURI(), configRoot.toUri());
      Assert.assertEquals(confFactory.getPhysicalScheme(), "file");

      // Valid path
      SimpleHDFSConfigStore store1 = confFactory.createConfigStore(new URI("simple-file:/d"));
      Assert.assertEquals(store1.getStoreURI().getScheme(), confFactory.getScheme());
      Assert.assertEquals(store1.getStoreURI().getAuthority(),
                          confFactory.getDefaultStoreURI().getAuthority());
      Assert.assertEquals(store1.getStoreURI().getPath(),
                          confFactory.getDefaultStoreURI().getPath());

      // Invalid path
      Config confConf2 =
          ConfigFactory.empty().withValue(SimpleHDFSConfigStoreFactory.DEFAULT_STORE_URI_KEY,
                                          ConfigValueFactory.fromAnyRef(testRoot.toString()));
      try {
        new SimpleHDFSConfigStoreFactory(confConf2);
        Assert.fail("Exception expected");
      }
      catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("Path does not appear to be a config store root"));
      }
    }
    finally {
      localFS.delete(testRoot, true);
    }
  }
}
