package gobblin.async;

/**
 * Exception for dispatching failures. By default, it is a fatal exception
 */
public class DispatchException extends Exception {
  private final boolean isFatalException;
  public DispatchException(String message, Exception e) {
    this(message, e, true);
  }

  public DispatchException(String message) {
    this(message, true);
  }

  public DispatchException(String message, Exception e, boolean isFatal) {
    super(message, e);
    isFatalException = isFatal;
  }

  public DispatchException(String message, boolean isFatal) {
    super(message);
    isFatalException = isFatal;
  }

  public boolean isFatal() {
    return isFatalException;
  }
}
