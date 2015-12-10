package gobblin.config.configstore;

public class ConfigStoreFactoryDoesNotExistsException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = -131707505927389860L;

  public ConfigStoreFactoryDoesNotExistsException(String message) {
    super(message);
  }
  
  public ConfigStoreFactoryDoesNotExistsException(Exception e) {
    super(e);
  }
}
