package gobblin.config.store.api;

public class ConfigStoreCreationException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -6021197312675836949L;

  public ConfigStoreCreationException(String message) {
    super(message);
  }
  
  public ConfigStoreCreationException(Exception e) {
    super(e);
  }
}
