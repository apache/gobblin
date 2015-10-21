package gobblin.dataset.config;

import java.util.*;

import com.typesafe.config.Config;

public interface ConfigStore {

  public String getLatestVersion();
  
  // DAI_ETL, DALI or Espresso
  public String getScheme();
  
  //
  public void loadConfigs();
  
  public void loadConfigs(String version);
  
  public Config getConfig(String urn);
  
  public Config getConfig(String urn, String version);
  
  public Map<String, Config> getTaggedConfig(String urn);
  
  public Map<String, Config> getTaggedConfig(String urn, String version);
  
  public List<String> getAssociatedTags(String urn);
  
  public List<String> getAssociatedTags(String urn, String version);
  
}
