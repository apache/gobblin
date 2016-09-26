package gobblin.util.request_allocation;

import java.util.Iterator;


/**
 * An {@link Iterator} over {@link Request} that also provides with the total resources used by all consumed entries.
 */
public interface AllocatedRequests<T extends Request<T>> extends Iterator<T> {
  /**
   * @return The total resources used by the elements consumed so far from this iterator.
   */
  ResourceRequirement totalResourcesUsed();
}
