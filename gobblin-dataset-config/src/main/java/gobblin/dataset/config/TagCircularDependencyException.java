package gobblin.dataset.config;

public class TagCircularDependencyException extends RuntimeException{

  /**
   * 
   */
  private static final long serialVersionUID = -164765448729513949L;
  public TagCircularDependencyException(String message){
    super(message);
  }
}
