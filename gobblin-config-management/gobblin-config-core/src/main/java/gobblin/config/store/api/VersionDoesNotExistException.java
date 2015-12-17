package gobblin.config.store.api;

import java.net.URI;


public class VersionDoesNotExistException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 2736458021800944664L;

  private final URI storeURI;
  private final String version;

  public VersionDoesNotExistException(URI storeURI, String version, String message) {
    super(String
        .format("failed to find the version %s in config store %s with message %s ", version, storeURI, message));
    this.storeURI = storeURI;
    this.version = version;
  }

  public VersionDoesNotExistException(URI storeURI, String version, Exception e) {
    super(e);
    this.storeURI = storeURI;
    this.version = version;
  }

  public URI getStoreURI() {
    return storeURI;
  }

  public String getVersion() {
    return version;
  }
}
