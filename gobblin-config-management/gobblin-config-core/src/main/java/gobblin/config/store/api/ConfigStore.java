package gobblin.config.store.api;

import java.net.URI;
import java.util.Collection;

import com.typesafe.config.Config;


/**
 * The ConfigStore interface used to describe a configuration store.
 * @author mitu
 *
 */
public interface ConfigStore {

  /**
   * @return the current version for that configuration store.
   */
  public String getCurrentVersion();

  /**
   * @return the configuration store root URI
   */
  public URI getStoreURI();

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the direct children URIs for input uri against input configuration version
   */
  public Collection<URI> getChildren(URI uri, String version) throws VersionDoesNotExistException;

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly imported URIs for input uri against input configuration version
   */
  public Collection<URI> getOwnImports(URI uri, String version) throws VersionDoesNotExistException;

  /**
   * @param uri - the uri relative to this configuration store
   * @param version - specify the configuration version in the configuration store.
   * @return - the directly specified configuration in com.typesafe.config.Config format for input uri 
   *  against input configuration version
   */
  public Config getOwnConfig(URI uri, String version) throws VersionDoesNotExistException;
}
