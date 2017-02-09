package gobblin.capability;

import java.util.Map;

import gobblin.annotation.Alpha;


/**
 * Describes an object that is aware of the capabilities it supports.
 */
@Alpha
public interface CapabilityAware {
  /**
   * Checks if this object supports the given Capability with the given properties.
   *
   * Implementers of this should always check if their super-class may happen to support a capability
   * before returning false!
   * @param c Capability being queried
   * @param properties Properties specific to the capability. Properties are capability specific.
   * @return True if this object supports the given capability + property settings, false if not
   */
  boolean supportsCapability(Capability c, Map<String, Object> properties);
}
