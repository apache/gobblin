/**
 *
 */
package gobblin.runtime.locks;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import gobblin.runtime.api.JobSpec;

/**
 * A factory for locks keyed on {@link JobSpec} . Typical usecase is to prevent the same job being
 * executed concurrently.
 */
public interface JobLockFactory<T extends JobLock> extends Closeable {

  /** Attempts to create a lock for the job. This method will block for a implementation-defined
   * default timeout. The timeout may be infinite!*/
  T getJobLock(JobSpec jobSpec) throws TimeoutException;

  /** Attempts to get a lock for the specified job within a given timeout */
  T getJobLock(JobSpec jobSpec, long timeoutDuration, TimeUnit timeoutUnit) throws TimeoutException;

}
