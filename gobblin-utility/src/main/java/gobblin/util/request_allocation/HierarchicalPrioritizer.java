package gobblin.util.request_allocation;

import java.util.Comparator;


public interface HierarchicalPrioritizer<T extends Request> extends Comparator<T> {
  int compareRequestors(Requestor<T> r1, Requestor<T> r2);
}
