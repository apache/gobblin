package gobblin.writer.http;

import java.util.Queue;

/**
 * An interface to build a async write request from a buffer of records
 *
 * @param <D> type of record
 * @param <RQ> type of request
 */
public interface AsyncWriteRequestBuilder<D, RQ> {
   AsyncWriteRequest<D, RQ> buildRequest(Queue<BufferedRecord<D>> buffer);
}
