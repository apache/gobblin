/**
 *
 */
package gobblin.runtime.locks;

import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.typesafe.config.Config;

/**
 * A factory class for {@link JobLockFactory} instances. It allows factories to be configured
 * using Gobblin instance configuration.
 *
 * <p>Implementations of this interface must define the default constructor</p>
 */
public interface JobLockFactoryManager<T extends JobLock, F extends JobLockFactory<T>> {

  /** Provides an instance of a job lock factory with the specified config. If an instance with
   * the same configuration (implementation-specific) already exists, the old instance may be
   * returned to avoid race condition. This behavior is implementation-specific. */
  F getJobLockFactory(Config sysCfg, Optional<Logger> log);
}
