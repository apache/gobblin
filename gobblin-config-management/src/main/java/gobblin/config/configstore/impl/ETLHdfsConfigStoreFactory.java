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
import gobblin.config.configstore.ConfigStoreFactory;


public class ETLHdfsConfigStoreFactory implements ConfigStoreFactory<ConfigStore> {

  private static final Logger LOG = Logger.getLogger(ETLHdfsConfigStoreFactory.class);
  private final ETLHdfsConfigStore store;
  private final FileSystem fs;
  private final boolean fsIsLocal;

  public static final String CONFIG_STORE_NAME = "_CONFIG_STORE";

  public ETLHdfsConfigStoreFactory(URI defaultConfigStoreRoot, boolean isLocal) throws ConfigStoreCreationException,
      IOException {
    this.fs = new Path(defaultConfigStoreRoot.getPath()).getFileSystem(new Configuration());
    this.fsIsLocal = isLocal;
    this.store = this.createConfigStore(defaultConfigStoreRoot);
  }

  @Override
  public String getScheme() {
    return "etl-hdfs";
  }

  @Override
  public ETLHdfsConfigStore getDefaultConfigStore() {
    return this.store;
  }

  private String getActualScheme() {
    if (this.fsIsLocal) {
      return "file";
    }

    return "hdfs";
  }

  @Override
  public ETLHdfsConfigStore createConfigStore(URI uri) throws ConfigStoreCreationException, IOException {
    if (!uri.getScheme().equals(this.getScheme())) {
      String errMesg =
          String.format("Input scheme %s does NOT match config store scheme %s", uri.getScheme(), this.getScheme());
      LOG.error(errMesg);
      throw new RuntimeException(errMesg);
    }

    try {
      URI adjusted = new URI(getActualScheme(), uri.getAuthority(), uri.getPath(), uri.getQuery(), uri.getFragment());
      URI root = this.findConfigStoreRoot(adjusted);
      return new ETLHdfsConfigStore(root);
    } catch (URISyntaxException e) {
      LOG.error("got error when constructing uri based on " + uri, e);
      throw new RuntimeException(e);
    }
  }

  private URI findConfigStoreRoot(URI input) throws ConfigStoreCreationException, IOException {
    Path p = new Path(input.getPath());

    while (p != null) {
      FileStatus[] fileStatus = this.fs.listStatus(p);
      for (FileStatus f : fileStatus) {
        if (!f.isDir() && f.getPath().getName().equals(CONFIG_STORE_NAME)) {
          return f.getPath().getParent().toUri();
        }
      }
      p = p.getParent();
    }

    throw new ConfigStoreCreationException("Can not find config store root in " + input);
  }

}
