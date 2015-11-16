package gobblin.config.configstore;

import java.util.Collection;

public interface ConfigStoreFactory {

  public Collection<String> getConfigStoreSchemes();
  
  public ConfigStore getConfigStore(String scheme);
}
