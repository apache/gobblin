package gobblin.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import junit.framework.Assert;
import lombok.extern.slf4j.Slf4j;

import gobblin.async.AsyncRequest;
import gobblin.async.AsyncRequestBuilder;
import gobblin.async.BufferedRecord;
import gobblin.async.Callback;
import gobblin.broker.BrokerConstants;
import gobblin.broker.SharedResourcesBrokerFactory;
import gobblin.broker.SharedResourcesBrokerImpl;
import gobblin.broker.SimpleScopeType;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.configuration.WorkUnitState;
import gobblin.http.HttpClient;
import gobblin.http.ResponseHandler;
import gobblin.http.ResponseStatus;
import gobblin.http.StatusType;
import gobblin.http.ThrottledHttpClient;
import gobblin.net.Request;
import gobblin.util.limiter.RateBasedLimiter;
import gobblin.util.limiter.broker.SharedLimiterFactory;


@Test
@Slf4j
public class AsyncHttpWriterTest {
  /**
   * Test successful writes of 4 records
   */
  @Test
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

  @Test
  public void testSuccessfulWritesWithLimiter () {
    MockThrottledHttpClient client = new MockThrottledHttpClient(createMockBroker());
    MockRequestBuilder requestBuilder = new MockRequestBuilder();
    MockResponseHandler responseHandler = new MockResponseHandler();
    MockAsyncHttpWriterBuilder builder = new MockAsyncHttpWriterBuilder(client, requestBuilder, responseHandler);
    TestAsyncHttpWriter asyncHttpWriter = new TestAsyncHttpWriter(builder);

    List<MockWriteCallback> callbacks = new ArrayList<>();

    for (int i = 0; i < 50; i++) {
      MockWriteCallback callback = new MockWriteCallback();
      callbacks.add(callback);
      asyncHttpWriter.write(new Object(), callback);
    }

    try {
      asyncHttpWriter.close();
    } catch (IOException e) {
      Assert.fail("Close failed");
    }

    // Assert all successful callbacks are invoked
    for (MockWriteCallback callback : callbacks) {
      Assert.assertTrue(callback.isSuccess);
    }

    Assert.assertTrue(client.getSendTimer().getCount() == 50);
    Assert.assertTrue(client.isCloseCalled);
  }

  private static SharedResourcesBroker createMockBroker() {
    Joiner JOINER = Joiner.on(".");
    Config config = ConfigFactory.parseMap(ImmutableMap.of(
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, SharedLimiterFactory.NAME, SharedLimiterFactory.LIMITER_CLASS_KEY), "qps",
        JOINER.join(BrokerConstants.GOBBLIN_BROKER_CONFIG_PREFIX, SharedLimiterFactory.NAME, RateBasedLimiter.Factory.QPS_KEY), "10"
    ));

    SharedResourcesBrokerImpl broker = SharedResourcesBrokerFactory.<SimpleScopeType>createDefaultTopLevelBroker(config, SimpleScopeType.GLOBAL.defaultScopeInstance());
    return broker;
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
    public void sendAsyncRequest(HttpUriRequest request, Callback<CloseableHttpResponse> callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
        throws IOException {
      isCloseCalled = true;
    }
  }

  class MockThrottledHttpClient extends ThrottledHttpClient<HttpUriRequest, CloseableHttpResponse> {
    boolean isCloseCalled = false;
    int attempts = 0;
    boolean shouldSendSucceed = true;
    public MockThrottledHttpClient (SharedResourcesBroker broker) {
      super (broker, "resource");
    }
    @Override
    public CloseableHttpResponse sendRequestImpl(HttpUriRequest request)
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

    @Override
    public void sendAsyncRequestImpl(HttpUriRequest request, Callback callback) {
      throw new UnsupportedOperationException();
    }
  }

  class MockRequestBuilder implements AsyncRequestBuilder<Object, HttpUriRequest> {
    @Override
    public AsyncRequest<Object, HttpUriRequest> buildRequest(Queue<BufferedRecord<Object>> buffer) {
      BufferedRecord<Object> item = buffer.poll();
      AsyncRequest<Object, HttpUriRequest> request = new AsyncRequest<>();
      request.markRecord(item, 1);
      request.setRawRequest(null);
      return request;
    }
  }

  class MockResponseHandler implements ResponseHandler<HttpUriRequest, CloseableHttpResponse> {
    volatile StatusType type = StatusType.OK;
    int attempts = 0;

    @Override
    public ResponseStatus handleResponse(Request<HttpUriRequest> request, CloseableHttpResponse response) {
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
    MockAsyncHttpWriterBuilder(HttpClient client, MockRequestBuilder requestBuilder,
        MockResponseHandler responseHandler) {
      this.client = client;
      this.asyncRequestBuilder = requestBuilder;
      this.responseHandler = responseHandler;
      this.state = new WorkUnitState();
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
