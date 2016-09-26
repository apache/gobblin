package gobblin.util.request_allocation;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;


/**
 * A {@link Requestor} that can provide an {@link Iterator} of {@link Request}s already sorted by the input
 * prioritizer. Allows push down of certain prioritizers to more efficient layers.
 */
public interface PushDownRequestor<T extends Request> extends Requestor<T> {
  /**
   * Return an {@link Iterator} of {@link Request}s already sorted by the input prioritizer.
   */
  Iterator<T> getRequests(Comparator<T> prioritizer) throws IOException;
}
