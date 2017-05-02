package gobblin.http;

/**
 * An interface to handle a response
 *
 * @param <RP> type of response
 */
public interface ResponseHandler<RP> {
  ResponseStatus handleResponse(RP response);
}
