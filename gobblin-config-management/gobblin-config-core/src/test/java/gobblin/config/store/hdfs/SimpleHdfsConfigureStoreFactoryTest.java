package gobblin.config.store.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.testng.Assert;
import org.testng.annotations.Test;

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
}
