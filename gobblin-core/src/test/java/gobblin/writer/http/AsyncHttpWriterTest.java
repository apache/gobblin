package gobblin.writer.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.testng.annotations.Test;

import com.typesafe.config.Config;

import junit.framework.Assert;

import gobblin.configuration.State;
import gobblin.http.HttpClient;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
import gobblin.http.StatusType;
import gobblin.writer.DataWriter;
import gobblin.writer.WriteCallback;
import gobblin.writer.WriteResponse;


@Test
public class AsyncHttpWriterTest {
  /**
   * Test successful writes of 4 records
   */
  public void testSuccessfulWrites() {
    MockHttpClient client = new MockHttpClient();
    MockRequestBuilder requestBuilder = new MockRequestBuilder();
    MockResponseHandler responseHandler = new MockResponseHandler();
    MockAsyncHttpWriterBuilder builder = new MockAsyncHttpWriterBuilder(client, requestBuilder, responseHandler);
    TestAsyncHttpWriter asyncHttpWriter = new TestAsyncHttpWriter(builder);

    List<MockWriteCallback> callbacks = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      callbacks.add(new MockWriteCallback());
    }
    asyncHttpWriter.write(new Object(), callbacks.get(0));
    asyncHttpWriter.write(new Object(), callbacks.get(1));
    asyncHttpWriter.write(new Object(), callbacks.get(2));

    try {
      asyncHttpWriter.flush();
    } catch (IOException e) {
      Assert.fail("Flush failed");
    }

    asyncHttpWriter.write(new Object(), callbacks.get(3));
    try {
      asyncHttpWriter.close();
    } catch (IOException e) {
      Assert.fail("Close failed");
    }

    // Assert all successful callbacks are invoked
    for (MockWriteCallback callback : callbacks) {
      Assert.assertTrue(callback.isSuccess);
    }

    Assert.assertTrue(client.isCloseCalled);
  }

  /**
   * Test failure triggered by client error. No retries
   */
  public void testClientError() {
    MockHttpClient client = new MockHttpClient();
    MockRequestBuilder requestBuilder = new MockRequestBuilder();
    MockResponseHandler responseHandler = new MockResponseHandler();
    MockAsyncHttpWriterBuilder builder = new MockAsyncHttpWriterBuilder(client, requestBuilder, responseHandler);
    TestAsyncHttpWriter asyncHttpWriter = new TestAsyncHttpWriter(builder);

    responseHandler.type = StatusType.CLIENT_ERROR;
    MockWriteCallback callback = new MockWriteCallback();
    asyncHttpWriter.write(new Object(), callback);

    boolean hasAnException = false;
    try {
      asyncHttpWriter.close();
    } catch (Exception e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException);
    Assert.assertFalse(callback.isSuccess);
    Assert.assertTrue(client.isCloseCalled);
    // No retries are done
    Assert.assertTrue(client.attempts == 1);
    Assert.assertTrue(responseHandler.attempts == 1);
  }

  /**
   * Test max attempts triggered by failing to send. Attempt 3 times
   */
  public void testMaxAttempts() {
    MockHttpClient client = new MockHttpClient();
    MockRequestBuilder requestBuilder = new MockRequestBuilder();
    MockResponseHandler responseHandler = new MockResponseHandler();
    MockAsyncHttpWriterBuilder builder = new MockAsyncHttpWriterBuilder(client, requestBuilder, responseHandler);
    TestAsyncHttpWriter asyncHttpWriter = new TestAsyncHttpWriter(builder);

    client.shouldSendSucceed = false;
    MockWriteCallback callback = new MockWriteCallback();
    asyncHttpWriter.write(new Object(), callback);

    boolean hasAnException = false;
    try {
      asyncHttpWriter.close();
    } catch (Exception e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException);
    Assert.assertFalse(callback.isSuccess);
    Assert.assertTrue(client.isCloseCalled);
    Assert.assertTrue(client.attempts == AsyncHttpWriter.DEFAULT_MAX_ATTEMPTS);
    Assert.assertTrue(responseHandler.attempts == 0);
  }

  /**
   * Test server error. Attempt 3 times
   */
  public void testServerError() {
    MockHttpClient client = new MockHttpClient();
    MockRequestBuilder requestBuilder = new MockRequestBuilder();
    MockResponseHandler responseHandler = new MockResponseHandler();
    MockAsyncHttpWriterBuilder builder = new MockAsyncHttpWriterBuilder(client, requestBuilder, responseHandler);
    TestAsyncHttpWriter asyncHttpWriter = new TestAsyncHttpWriter(builder);

    responseHandler.type = StatusType.SERVER_ERROR;
    MockWriteCallback callback = new MockWriteCallback();
    asyncHttpWriter.write(new Object(), callback);

    boolean hasAnException = false;
    try {
      asyncHttpWriter.close();
    } catch (Exception e) {
      hasAnException = true;
    }
    Assert.assertTrue(hasAnException);
    Assert.assertFalse(callback.isSuccess);
    Assert.assertTrue(client.isCloseCalled);
    Assert.assertTrue(client.attempts == AsyncHttpWriter.DEFAULT_MAX_ATTEMPTS);
    Assert.assertTrue(responseHandler.attempts == AsyncHttpWriter.DEFAULT_MAX_ATTEMPTS);
  }

  class MockHttpClient implements HttpClient<HttpUriRequest, CloseableHttpResponse> {
    boolean isCloseCalled = false;
    int attempts = 0;
    boolean shouldSendSucceed = true;

    @Override
    public CloseableHttpResponse sendRequest(HttpUriRequest request)
        throws IOException {
      attempts++;
      if (shouldSendSucceed) {
        // We won't consume the response anyway
        return null;
      }
      throw new IOException("Send failed");
    }

    @Override
    public void close()
        throws IOException {
      isCloseCalled = true;
    }
  }

  class MockRequestBuilder implements AsyncWriteRequestBuilder<Object, HttpUriRequest> {

    @Override
    public AsyncWriteRequest<Object, HttpUriRequest> buildWriteRequest(Queue<BufferedRecord<Object>> buffer) {
      BufferedRecord<Object> item = buffer.poll();
      AsyncWriteRequest<Object, HttpUriRequest> request = new AsyncWriteRequest<>();
      request.markRecord(item, 1);
      request.setRawRequest(null);
      return request;
    }
  }

  class MockResponseHandler implements ResponseHandler<CloseableHttpResponse> {
    volatile StatusType type = StatusType.OK;
    int attempts = 0;

    @Override
    public ResponseStatus handleResponse(CloseableHttpResponse response) {
      attempts++;
      switch (type) {
        case OK:
          return new ResponseStatus(StatusType.OK);
        case CLIENT_ERROR:
          return new ResponseStatus(StatusType.CLIENT_ERROR);
        case SERVER_ERROR:
          return new ResponseStatus(StatusType.SERVER_ERROR);
      }
      return null;
    }
  }

  class MockWriteCallback implements WriteCallback<Object> {
    boolean isSuccess = false;

    @Override
    public void onSuccess(WriteResponse<Object> writeResponse) {
      isSuccess = true;
    }

    @Override
    public void onFailure(Throwable throwable) {
      isSuccess = false;
    }
  }

  class MockAsyncHttpWriterBuilder extends AsyncHttpWriterBuilder<Object, HttpUriRequest, CloseableHttpResponse> {
    MockAsyncHttpWriterBuilder(MockHttpClient client, MockRequestBuilder requestBuilder,
        MockResponseHandler responseHandler) {
      this.client = client;
      this.asyncRequestBuilder = requestBuilder;
      this.responseHandler = responseHandler;
      this.state = new State();
      this.queueCapacity = 2;
    }

    @Override
    public DataWriter<Object> build()
        throws IOException {
      return null;
    }

    @Override
    public AsyncHttpWriterBuilder<Object, HttpUriRequest, CloseableHttpResponse> fromConfig(Config config) {
      return null;
    }
  }

  class TestAsyncHttpWriter extends AsyncHttpWriter<Object, HttpUriRequest, CloseableHttpResponse> {

    public TestAsyncHttpWriter(AsyncHttpWriterBuilder builder) {
      super(builder);
    }
  }
}
