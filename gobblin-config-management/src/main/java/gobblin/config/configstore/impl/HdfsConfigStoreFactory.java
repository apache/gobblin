package gobblin.config.configstore.impl;

import java.net.URI;

import gobblin.config.configstore.ConfigStore;
import gobblin.config.configstore.ConfigStoreFactory;

public class HdfsConfigStoreFactory implements ConfigStoreFactory<ConfigStore>{

  @Override
  public String getScheme() {
    return "hdfs";
  }

  @Override
  public ConfigStore getDefaultConfigStore() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ConfigStore createConfigStore(URI uri) {
    // TODO Auto-generated method stub
    return null;
  }

}
