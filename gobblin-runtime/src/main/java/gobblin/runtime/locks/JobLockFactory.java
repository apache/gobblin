/**
 *
 */
package gobblin.runtime.locks;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;

import gobblin.runtime.api.JobSpec;

/**
 * A factory for locks keyed on {@link JobSpec} . Typical usecase is to prevent the same job being
 * executed concurrently.
 */
public interface JobLockFactory<T extends JobLock> extends Closeable {

  /** Creates a lock for the specified job. Lock is not acquired. */
  T getJobLock(JobSpec jobSpec) throws TimeoutException;

}
