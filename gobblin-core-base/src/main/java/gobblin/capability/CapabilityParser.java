package gobblin.capability;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import gobblin.configuration.State;


/**
 * Represents a class that can parse a set of capabilities out of job/task configuration.
 */
public abstract class CapabilityParser {
  /**
   * Initialize the parser.
   * @param supportedCapabilities Collection of capabilities this parser can extract
   */
  CapabilityParser(Set<Capability> supportedCapabilities) {
    this.supportedCapabilities = supportedCapabilities;
  }

  /**
   * Return a set of CapabilityRecords for a given branch by looking through job config and returning a set of
   * CapabilityRecords for each capability the object knows how to parser.
   *
   * One CapabilityRecord should be returned for each supported capability.
   * @param taskState Overall task state
   * @param numBranches Number of branches in the task
   * @param branch Branch number to retrieve capabilities for
   * @return Collection of capability records showing whether a given capability is enabled for the branch and any associated properties.
   *
   */
  public abstract Collection<CapabilityRecord> parseForBranch(State taskState, int numBranches, int branch);

  /**
   * @return Return the capabilities this parser supports
   */
  public Set<Capability> getSupportedCapabilities() {
    return supportedCapabilities;
  }

  /**
   * Describes a Capability with associated configuration parameters
   */
  public static class CapabilityRecord {
    private Capability capability;
    private final boolean supportsCapability;
    private final Map<String, Object> parameters;

    public CapabilityRecord(Capability capability, boolean supportsCapability, Map<String, Object> parameters) {
      this.supportsCapability = supportsCapability;
      this.parameters = parameters;
      this.capability = capability;
    }

    /**
     * Return whether or not the capability is supported
     * @return True if supported, false if not
     */
    public boolean supportsCapability() {
      return supportsCapability;
    }

    /**
     * Synonym for supportsCapability()
     * @return
     */
    public boolean isConfigured() {
      return supportsCapability;
    }

    /**
     * Retrieve any parameters associated with the capability
     * @return
     */
    public Map<String, Object> getParameters() {
      return parameters;
    }

    public Capability getCapability() {
      return capability;
    }

    @Override
    public String toString() {
      return "CapabilityRecord{" + "capability=" + capability + ", supportsCapability=" + supportsCapability + ", parameters="
          + parameters + '}';
    }
  }

  private final Set<Capability> supportedCapabilities;
}
