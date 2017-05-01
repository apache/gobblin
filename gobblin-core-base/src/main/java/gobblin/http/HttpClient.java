package gobblin.http;

import java.io.Closeable;
import java.io.IOException;


/**
 * An interface to send a request
 *
 * @param <RQ> type of request
 * @param <RP> type of response
 */
public interface HttpClient<RQ, RP> extends Closeable {
  /**
   * Send request synchronously
   */
  RP sendRequest(RQ request) throws IOException;
}

