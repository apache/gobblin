package gobblin.dataset.config_api;

import com.typesafe.config.Config;

public interface ConfigStore {

  public String getLatestVersion();
  
  public String getScheme();
  
  public void loadConfigs();
  
  public Config getConfig(String urn);
}
