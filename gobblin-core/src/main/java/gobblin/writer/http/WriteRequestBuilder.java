package gobblin.writer.http;

/**
 * An interface to build a write request from a record
 *
 * @param <D> type of record
 * @param <RQ> type of request
 */
public interface WriteRequestBuilder<D, RQ> {
  WriteRequest<RQ> buildRequest(D record);
}
