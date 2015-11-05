package gobblin.dataset.configstore;

import java.net.URI;

import com.typesafe.config.Config;

public interface ConfigStoreWithResolution extends ConfigStore{
  
  public Config getResolvedConfig(URI uri);

}
