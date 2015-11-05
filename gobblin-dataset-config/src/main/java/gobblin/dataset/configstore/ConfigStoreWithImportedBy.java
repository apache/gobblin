package gobblin.dataset.configstore;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

import com.typesafe.config.Config;

public interface ConfigStoreWithImportedBy extends ConfigStore{
  public Collection<URI> getImportedBy(URI uri);
  
  public Map<URI, Config> getConfigsImportedBy(URI uri);
}
