package gobblin.runtime.locks;

/**
 * The exception thrown when a {@link JobLock} cannot be initialized, acquired,
 * release, etc.
 *
 * @author joelbaranick
 */
public class JobLockException extends Exception {
  public JobLockException(Throwable cause) {
        super(cause);
    }

  public JobLockException(String message, Throwable cause) {
        super(message, cause);
    }

  public JobLockException(String message) {
        super(message);
    }
}
