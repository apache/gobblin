package gobblin.yarn;

/**
 * An enumeration of Helix message sub types.
 *
 * @author ynli
 */
public enum HelixMessageSubTypes {

  /**
   * This type is for messages sent when the ApplicationMaster is to be shutdown.
   */
  APPLICATION_MASTER_SHUTDOWN,

  /**
   * This type is for messages sent when the containers are to be shutdown.
   */
  CONTAINER_SHUTDOWN
}
