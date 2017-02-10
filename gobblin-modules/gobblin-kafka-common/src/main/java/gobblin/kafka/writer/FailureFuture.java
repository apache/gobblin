package gobblin.kafka.writer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Future that always throws an exception when queried
 * @param <T> Type of variable that would be returned
 */
public class FailureFuture<T> implements Future<T> {
  private final Throwable exception;

  /**
   * Build new future.
   * @param exception Exception that should be thrown when get() is called
   */
  public FailureFuture(Throwable exception) {
    this.exception = exception;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    throw new ExecutionException(exception);
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    return get();
  }
}
