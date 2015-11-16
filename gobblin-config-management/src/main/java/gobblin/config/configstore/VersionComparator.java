package gobblin.config.configstore;

import java.util.Collection;

public interface VersionComparator<V> {

  public V getCurrentVersion(Collection<V> versions);
}
