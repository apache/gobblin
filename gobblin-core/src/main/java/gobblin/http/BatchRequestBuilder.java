package gobblin.http;

import java.util.Collection;


/**
 * An interface to build a request from a collection of record
 *
 * @param <D> type of record
 * @param <RQ> type of request
 */
public interface BatchRequestBuilder<D, RQ> {
  RQ buildRequest(Collection<D> records);
}
