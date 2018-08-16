package org.apache.gobblin.dataset;

import org.apache.gobblin.configuration.State;


/**
 * A resolver transforms an existing {@link Descriptor} to a new one
 */
public interface DescriptorResolver {
  /**
   * Given raw Gobblin descriptor, resolve a job specific descriptor
   *
   * @param raw the original descriptor
   * @param state configuration that helps resolve job specific descriptor
   * @return resolved descriptor for the job or {@code null} if failed to resolve
   */
  Descriptor resolve(Descriptor raw, State state);
}
