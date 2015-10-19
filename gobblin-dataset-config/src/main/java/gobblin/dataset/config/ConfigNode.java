package gobblin.dataset.config;

import java.util.List;

import com.typesafe.config.Config;

public interface ConfigNode {

  public Config getConfig();
  
  public ConfigNode getParent();
  
  public boolean isTagable();
  
  public List<ConfigNode> getAssociatedTags();
}
