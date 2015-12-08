package gobblin.config.configstore;

public class VersionDoesNotExistException extends Exception {

  /**
   * 
   */
  private static final long serialVersionUID = 2736458021800944664L;

  public VersionDoesNotExistException(String message) {
    super(message);
  }
  
  public VersionDoesNotExistException(Exception e) {
    super(e);
  }
}
