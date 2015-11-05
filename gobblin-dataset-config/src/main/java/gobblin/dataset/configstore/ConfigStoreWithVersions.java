package gobblin.dataset.configstore;

import java.util.Collection;

public interface ConfigStoreWithVersions {
  /**
   * @return the latest configuration version in this ConfigStore
   */
  public String getLatestVersion();
  
  public Collection<String> getVersions();
  
  public ConfigStore getConfigStore(String version);

}
