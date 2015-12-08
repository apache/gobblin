package gobblin.config.configstore.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreCreationException;
import gobblin.config.configstore.ConfigStoreFactory;


public class ETLHdfsConfigStoreFactory implements ConfigStoreFactory<ConfigStore> {

  public static final String SCHEME_NAME = "etl-hdfs";
  public static final String CONFIG_STORE_NAME = "_CONFIG_STORE";
  private static final Logger LOG = Logger.getLogger(ETLHdfsConfigStoreFactory.class);

  @Override
  public String getScheme() {
    return SCHEME_NAME;
  }

  private String getActualScheme() {
    return "hdfs";
  }

  @Override
  /**
   * @param uri - logic hdfs URI, the scheme name could be different than actual hdfs scheme name, 
   *  example: new URI("etl-hdfs://eat1-nertznn01.grid.linkedin.com:9000/user/mitu/HdfsBasedConfigTest")
   * @return ETLHdfsConfigStore determined by input uri 
   *  The logic to determine the store root is by back tracing the input uri, the path which contains "_CONFIG_STORE" is the root
   */
  public ETLHdfsConfigStore createConfigStore(URI uri) throws ConfigStoreCreationException {
    FileSystem fs;
    try {
      fs = new Path(uri.getPath()).getFileSystem(new Configuration());
    } catch (IOException e1) {
      LOG.error("got IOException when constructing uri based on " + uri, e1);
      throw new ConfigStoreCreationException(e1);
    }
    
    if (!uri.getScheme().equals(this.getScheme())) {
      String errMesg =
          String.format("Input scheme %s does NOT match config store scheme %s", uri.getScheme(), this.getScheme());
      LOG.error(errMesg);
      throw new ConfigStoreCreationException(errMesg);
    }

    try {
      URI adjusted = new URI(getActualScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), uri.getFragment());
      URI physical_root = this.findConfigStoreRoot(adjusted, fs);
      URI logical_root =
          new URI(getScheme(), physical_root.getAuthority(), physical_root.getPath(), physical_root.getQuery(),
              physical_root.getFragment());
      return new ETLHdfsConfigStore(physical_root, logical_root);
    } catch (URISyntaxException e) {
      LOG.error("got URISyntaxException when constructing uri based on " + uri, e);
      throw new ConfigStoreCreationException(e);
    } catch (IOException e) {
      LOG.error("got IOException when constructing uri based on " + uri, e);
      throw new ConfigStoreCreationException(e);
    }
  }

  private URI findConfigStoreRoot(URI input, FileSystem fs) throws ConfigStoreCreationException, IOException {
    Path p = new Path(input.getPath());

    while (p != null) {
      // URI input may missing the version information as client do NOT know the version, so need to 
      // find the existing parent without list Non existing path
      if (fs.exists(p)) {
        FileStatus[] fileStatus = fs.listStatus(p);
        for (FileStatus f : fileStatus) {
          if (!f.isDir() && f.getPath().getName().equals(CONFIG_STORE_NAME)) {
            String parent = f.getPath().getParent().toString();

            try {
              return new URI(parent);
            } catch (URISyntaxException e) {
              // Should not come here
              e.printStackTrace();
            }
          }
        }
      }
      p = p.getParent();
    }

    throw new ConfigStoreCreationException("Can not find config store root in " + input);
  }

}
