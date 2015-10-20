package gobblin.dataset.config_api;

public class ConfigVersionMissMatchException extends IllegalArgumentException {

  /**
   * 
   */
  private static final long serialVersionUID = 2824330818150599343L;
  
  public ConfigVersionMissMatchException(String message){
    super(message);
  }

}
