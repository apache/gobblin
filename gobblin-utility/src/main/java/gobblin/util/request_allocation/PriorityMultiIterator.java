package gobblin.util.request_allocation;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

import lombok.Data;


/**
 * An {@link Iterator} that takes multiple input {@link Iterator}s each of whose elements are ordered by the input
 * {@link Comparator} and iterates over the elements in all input iterators in a globally ordered way.
 *
 * Note: this class does not check whether the input {@link Iterator}s are ordered correctly, so it is package-private
 * to prevent misuse.
 */
class PriorityMultiIterator<T> implements Iterator<T> {

  private final PriorityQueue<TAndIterator> queue;
  private final Comparator<TAndIterator> actualComparator;

  public PriorityMultiIterator(Collection<Iterator<T>> orderedIterators, final Comparator<T> prioritizer) {
    this.actualComparator = new Comparator<TAndIterator>() {
      @Override
      public int compare(TAndIterator o1, TAndIterator o2) {
        return prioritizer.compare(o1.getT(), o2.getT());
      }
    };
    this.queue = new PriorityQueue<>(orderedIterators.size(), this.actualComparator);
    for (Iterator<T> iterator : orderedIterators) {
      if (iterator.hasNext()) {
        this.queue.offer(new TAndIterator(iterator.next(), iterator));
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !this.queue.isEmpty();
  }

  @Override
  public T next() {
    TAndIterator nextTAndIterator = this.queue.poll();
    if (nextTAndIterator.getIterator().hasNext()) {
      this.queue.offer(new TAndIterator(nextTAndIterator.getIterator().next(), nextTAndIterator.getIterator()));
    }
    return nextTAndIterator.getT();
  }

  @Data
  private class TAndIterator {
    private final T t;
    private final Iterator<T> iterator;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

}
