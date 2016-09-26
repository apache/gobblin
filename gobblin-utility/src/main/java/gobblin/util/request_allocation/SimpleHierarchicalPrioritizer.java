package gobblin.util.request_allocation;

import java.util.Comparator;

import lombok.AllArgsConstructor;


@AllArgsConstructor
public class SimpleHierarchicalPrioritizer<T extends Request<T>> implements HierarchicalPrioritizer<T> {

  private final Comparator<Requestor<T>> requestorComparator;
  private final Comparator<T> requestComparator;

  @Override
  public int compareRequestors(Requestor<T> r1, Requestor<T> r2) {
    return this.requestorComparator.compare(r1, r2);
  }

  @Override
  public int compare(T o1, T o2) {
    int requestorComparison = this.requestorComparator.compare(o1.getRequestor(), o2.getRequestor());
    if (requestorComparison != 0) {
      return requestorComparison;
    }
    return this.requestComparator.compare(o1, o2);
  }
}
