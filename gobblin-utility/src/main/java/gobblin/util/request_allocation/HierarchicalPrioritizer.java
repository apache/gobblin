package gobblin.util.request_allocation;

import java.util.Comparator;


/**
 * A {@link Comparator} for {@link Request}s that can also compare {@link Requestor}s, and which guarantees that
 * given {@link Request}s r1, r2, then r1.getRequestor > r2.getRequestor implies r1 > r2.
 */
public interface HierarchicalPrioritizer<T extends Request> extends Comparator<T> {
  int compareRequestors(Requestor<T> r1, Requestor<T> r2);
}
