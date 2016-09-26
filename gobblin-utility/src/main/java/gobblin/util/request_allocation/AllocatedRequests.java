package gobblin.util.request_allocation;

import java.util.Iterator;


public interface AllocatedRequests<T> extends Iterator<T> {
  /**
   * @return The total resources used by the elements consumed so far from this iterator.
   */
  ResourceRequirement totalResourcesUsed();
}
