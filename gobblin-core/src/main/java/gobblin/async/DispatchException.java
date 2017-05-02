package gobblin.async;

/**
 * Exception for dispatching failures
 */
public class DispatchException extends Exception {
  public DispatchException(String message, Exception e) {
    super(message, e);
  }

  public DispatchException(String message) {
    super(message);
  }

  public boolean isFatal() {
    return true;
  }
}
