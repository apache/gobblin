package gobblin.config.client;

import java.net.URI;

import com.typesafe.config.Config;

import gobblin.config.configstore.ConfigStore;

public interface ConfigClient {

  /**
   * @param cs
   * @param uri - the uri relative to the input ConfigStore
   * @return
   */
  public Config getConfig(ConfigStore cs, URI uri);
}
