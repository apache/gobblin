package gobblin.config.store.api;

import java.net.URI;


public class ConfigStoreCreationException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -6021197312675836949L;
  
  private final URI storeURI;

  public ConfigStoreCreationException(URI storeURI, String message) {
    super(String.format("failed to create config store %s with message %s", storeURI, message));
    this.storeURI = storeURI;
  }

  public ConfigStoreCreationException(URI storeURI, Exception e) {
    super(e);
    this.storeURI = storeURI;
  }
  
  public URI getStoreURI(){
    return this.storeURI;
  }
}
