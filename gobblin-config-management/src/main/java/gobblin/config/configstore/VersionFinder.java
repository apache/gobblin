package gobblin.config.configstore;

import java.util.Collection;


public interface VersionFinder<V> {

  public V getCurrentVersion(Collection<V> versions);
}
