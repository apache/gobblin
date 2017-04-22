package gobblin.http;


/**
 * An interface to build a request from a record
 *
 * @param <D> type of record
 * @param <RQ> type of request
 */
public interface RequestBuilder<D, RQ> {
  RQ buildRequest(D record);
}

