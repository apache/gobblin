package gobblin.writer.http;

import java.io.IOException;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gobblin.http.HttpClient;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
import gobblin.writer.WriteResponse;


public class AsyncHttpWriter<D, RQ, RP> extends AbstractAsyncDataWriter<D> {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncHttpWriter.class);
  public static final int DEFAULT_MAX_TRIES = 3;

  private final HttpClient<RQ, RP> client;
  private final ResponseHandler<RP> responseHandler;
  private final AsyncWriteRequestBuilder<D, RQ> requestBuilder;
  private final int maxTries;

  public AsyncHttpWriter(AsyncHttpWriterBaseBuilder builder) {
    super(builder.getQueueCapacity());
    this.client = builder.getClient();
    this.requestBuilder = builder.getAsyncRequestBuilder();
    this.responseHandler = builder.getResponseHandler();
    this.maxTries = builder.getMaxTries();
  }

  @Override
  protected int dispatch(Queue<BufferedRecord<D>> buffer) throws Throwable {
    AsyncWriteRequest<D, RQ> asyncWriteRequest = requestBuilder.buildRequest(buffer);
    if (asyncWriteRequest == null) {
      return 0;
    }

    RQ rawRequest = asyncWriteRequest.getRawRequest();
    RP response;

    int i = 0;
    while (i < maxTries) {
      try {
        response = client.sendRequest(rawRequest);
      } catch (IOException e) {
        // Retry
        i++;
        if (i == maxTries) {
          asyncWriteRequest.onFailure(e);
          LOG.error("Write failed on IOException", e);
          break;
        } else {
          continue;
        }
      }

      ResponseStatus status = responseHandler.handleResponse(response);
      int statusCode = status.getStatusCode();
      if (statusCode >= 200 && statusCode < 400) {
        // Write succeeds
        asyncWriteRequest.onSuccess(WriteResponse.EMPTY);
        return asyncWriteRequest.getRecordCount();
      } else if (statusCode >= 400 && statusCode < 500) {
        // Client error. Fail!
        LOG.error("Write failed on invalid request");
        return -1;
      } else {
        // Server side error. Retry
        i++;
      }
    }

    LOG.error("Write failed after " + maxTries + " tries");
    return -1;
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    client.close();
  }
}
