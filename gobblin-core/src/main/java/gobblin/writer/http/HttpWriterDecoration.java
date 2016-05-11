/**
 *
 */
package gobblin.writer.http;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Defines the main extension points for the {@link AbstractHttpWriter}.
 * @param D     the type of the data records
 */
public interface HttpWriterDecoration<D> {

  /** An extension point to select the HTTP server to connect to. */
  HttpHost chooseServerHost();

  /**
   * A callback triggered before attempting to connect to a new host. Subclasses can override this
   * method to customize the connect logic.
   * For example, they can implement OAuth authentication.*/
  void onConnect(HttpHost serverHost) throws IOException;

  /**
   * A callback that allows the subclasses to customize the construction of an HTTP request based on
   * incoming records. Customization may include, setting the URL, headers, buffering, etc.
   *
   * @param record      the new record to be written
   * @param request     the current request object; if absent the implementation is responsible of
   *                    allocating a new object
   * @return the current request object; if absent no further processing will happen
   */
  Optional<HttpUriRequest> onNewRecord(D record, Optional<HttpUriRequest> request);

  /**
   * An extension point to send the actual request to the remote server.
   * @param  request         the request to be sent
   * @return a future that allows access to the response. Response may be retrieved synchronously or
   *         asynchronously.
   */
  ListenableFuture<HttpResponse> sendRequest(HttpUriRequest request) throws IOException ;

  /**
   * Customize the waiting for an HTTP response. Can add timeout logic.
   * @param responseFuture  the future object of the last sent request
   */
  void waitForResponse(ListenableFuture<HttpResponse> responseFuture);

  /**
   * Processes the response
   * @throws  IOException if there was a problem reading the response
   * @throws  UnexpectedResponseException if the response was unexpected
   */
  void processResponse(HttpResponse response) throws IOException, UnexpectedResponseException;

}
