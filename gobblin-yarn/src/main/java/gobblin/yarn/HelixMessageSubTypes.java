package gobblin.yarn;

/**
 * An enumeration of Helix message sub types.
 *
 * @author ynli
 */
public enum HelixMessageSubTypes {

  /**
   * This type is for messages sent when the {@link GobblinApplicationMaster} is to be shutdown.
   */
  APPLICATION_MASTER_SHUTDOWN,

  /**
   * This type is for messages sent when the {@link GobblinWorkUnitRunner}s are to be shutdown.
   */
  WORK_UNIT_RUNNER_SHUTDOWN
}
