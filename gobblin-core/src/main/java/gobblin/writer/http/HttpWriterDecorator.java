/**
 *
 */
package gobblin.writer.http;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;


/**
 * Common parent for {@link AbstractHttpWriter} decorators. Delegates extension methods to another
 * implementation and simplifies the overriding of only selected methods
 */
public abstract class HttpWriterDecorator<D> implements HttpWriterDecoration<D> {

  private final HttpWriterDecoration<D> _fallback;

  public HttpWriterDecorator(HttpWriterDecoration<D> fallback) {
    Preconditions.checkNotNull(fallback);
    this._fallback = fallback;
  }

  protected HttpWriterDecoration<D> getFallback() {
    return this._fallback;
  }

  @Override
  public HttpHost chooseServerHost() {
    return getFallback().chooseServerHost();
  }

  @Override
  public void onConnect(HttpHost serverHost) throws IOException {
    getFallback().onConnect(serverHost);
  }

  @Override
  public Optional<HttpUriRequest> onNewRecord(D record, Optional<HttpUriRequest> request) {
    return getFallback().onNewRecord(record, request);
  }

  @Override
  public ListenableFuture<HttpResponse> sendRequest(HttpUriRequest request) throws IOException {
    return getFallback().sendRequest(request);
  }

  @Override
  public void waitForResponse(ListenableFuture<HttpResponse> responseFuture) {
    getFallback().waitForResponse(responseFuture);
  }

  @Override
  public void processResponse(HttpResponse response) throws IOException, UnexpectedResponseException {
    getFallback().processResponse(response);
  }

}
