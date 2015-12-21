package gobblin.config.client.api;

import gobblin.config.store.api.ConfigStoreWithStableVersioning;

/**
 * This version specifies the behavior expected by the client application when making repeated
 * calls to the fetch the configuration object for the same config key and version. This interface
 * is closely associated with the {@link ConfigStoreWithStableVersioning} API.
 *
 * <p>The semantic of each policy is documented with each constant.
 *
 * <p> Here is the table that summarizes the expected client library behavior depending on the
 * VersionStabilityPolicy and ConfigStoreWithStableVersioning support from a store.
 * <table>
 *   <tr><th>VersionStabilityPolicy/ConfigStoreWithStableVersioning</th><th>No</th><th>Yes</th></tr>
 *   <tr><th>{@link #CROSS_JVM_STABILITY}</th><td>ERROR</td><td>WeakCache</td></tr>
 *   <tr><th>{@link #STRONG_LOCAL_STABILITY}</th><td>StrongCache</td><td>WeakCache</td></tr>
 *   <tr><th>{@link #WEAK_LOCAL_STABILITY}</th><td>WeakCache</td><td>WeakCache</td></tr>
 *   <tr><th>{@link #READ_FRESHEST}</th><td>NoCache</td><td>WeakCache</td></tr>
 * </table>
 *
 * <ul>
 *   <li>ERROR means that the client library should throw an exception because the requested
 *       VersionStabilityPolicy cannot be supported</li>
 *   <li>WeakCache means that the client library may cache in memory configs that have been already
 *       read for performance reasons and if memory allows it.</li>
 *   <li>StrongCache means that the client library should always cache in memory the read configs to
 *       guarantee the requested VersionStabilityPolicy</li>
 *   <li>NoCache means that the client library should never cache the read configs.</li>
 * </ul>
 */
public enum VersionStabilityPolicy {
  /** Reading the same config key and version from different JVMs must return the same result. */
  CROSS_JVM_STABILITY,
  /** Reading the same config key and version from the same JVMs must return the same result. */
  STRONG_LOCAL_STABILITY,
  /**
   * The application does not depend on getting the same config  for the same key and version but
   * the client library may use caching to improve performance. This means that the application
   * may read a stale config if the underlying store does not support stable versioning. */
  WEAK_LOCAL_STABILITY,
  /**
   * The application needs to read the most recent config if the underlying store does not support
   * stable versioning.
   */
  READ_FRESHEST
}
