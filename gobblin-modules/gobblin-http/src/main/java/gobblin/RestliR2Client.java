package gobblin;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.linkedin.common.callback.Callbacks;
import com.linkedin.common.util.None;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;

import gobblin.http.HttpClient;


public class RestliR2Client implements HttpClient<RestRequest, RestResponse> {
  private final Client client;

  public RestliR2Client(Client client) {
    this.client = client;
  }

  @Override
  public RestResponse sendRequest(RestRequest request)
      throws IOException {
    Future<RestResponse> responseFuture = client.restRequest(request);
    RestResponse response;
    try {
      response = responseFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return response;
  }

  @Override
  public void close()
      throws IOException {
    client.shutdown(Callbacks.<None>empty());
  }
}
