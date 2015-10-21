package gobblin.dataset.config;

import java.util.*;
import com.typesafe.config.Config;

public class ConfigAccessor {

  private String configVersion;
  private ConfigStore configStore;
  
  public Config getConfig(String urn){
    return configStore.getConfig(urn, configVersion);
  }
  
  public Map<String, Config > getTaggedConfig(String urn){
    return this.configStore.getTaggedConfig(urn, configVersion);
  }
  
  public List<String> getAssociatedTags(String urn){
    return this.configStore.getAssociatedTags(urn, configVersion);
  }
}
