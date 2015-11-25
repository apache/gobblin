package gobblin.config.configstore;

import java.net.URI;
import java.util.Collection;
import java.util.Map;


public interface ImportMappings {
  /**
   * 
   * @return java.util.Map object. Keys in the Map is the relative URI to the configuration store/version
   * Values in the Map is the directly imported URIs by the corresponding keys
   */
  public Map<URI, Collection<URI>> getImportMapping();

  /**
   * @return java.util.Map object. Keys in the Map is the relative URI to the configuration store/version
   * Values in the Map is the URI which directly imported the corresponding keys
   */
  public Map<URI, Collection<URI>> getImportedByMapping();

  /**
   * @return - java.util.Map object. Keys in the Map is the relative URI to the configuration store/version.
   * The values in the Map is the directly and indirectly imported URIs followed the imported path for corresponding keys
   */
  public Map<URI, Collection<URI>> getImportMappingRecursively();

  /**
   * @return java.util.Map object. Keys in the Map is the relative URI to the configuration store/version
   * Values in the Map is the URI which directly or indirectly imported the corresponding keys
   */
  public Map<URI, Collection<URI>> getImportedByMappingRecursively();
}
