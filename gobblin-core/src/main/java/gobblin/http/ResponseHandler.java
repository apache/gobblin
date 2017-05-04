package gobblin.http;

import java.io.IOException;


/**
 * An interface to handle a response
 *
 * @param <RP> type of response
 */
public interface ResponseHandler<RP> {
  void handleResponse(RP response) throws IOException;
}
