package gobblin.config.configstore.impl;

public class VersionDoesNotExistException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 2736458021800944664L;

  public VersionDoesNotExistException(String message) {
    super(message);
  }
}
