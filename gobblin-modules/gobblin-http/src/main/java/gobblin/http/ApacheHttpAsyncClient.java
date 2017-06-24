package gobblin.http;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import gobblin.async.Callback;
import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.utils.HttpUtils;

/**
 * An asynchronous {@link HttpClient} which sends {@link HttpUriRequest} and registers a callback.
 * It encapsulates a {@link CloseableHttpClient} instance to send the {@link HttpUriRequest}
 *
 * {@link CloseableHttpAsyncClient} is used
 */
@Slf4j
public class ApacheHttpAsyncClient extends ThrottledHttpClient<HttpUriRequest, HttpResponse>  {
  private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpClient.class);

  public static final String HTTP_CONN_MANAGER = "connMgrType";
  public static final String POOLING_CONN_MANAGER_MAX_TOTAL_CONN = "connMgr.pooling.maxTotalConn";
  public static final String POOLING_CONN_MANAGER_MAX_PER_CONN = "connMgr.pooling.maxPerConn";
  public static final String REQUEST_TIME_OUT_MS_KEY = "reqTimeout";
  public static final String CONNECTION_TIME_OUT_MS_KEY = "connTimeout";


  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(REQUEST_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(10L))
          .put(CONNECTION_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(10L))
          .put(HTTP_CONN_MANAGER, ApacheHttpClient.ConnManager.POOLING.name())
          .put(POOLING_CONN_MANAGER_MAX_TOTAL_CONN, 20)
          .put(POOLING_CONN_MANAGER_MAX_PER_CONN, 2)
          .build());

  private final CloseableHttpAsyncClient client;

  public ApacheHttpAsyncClient(HttpAsyncClientBuilder builder, Config config, SharedResourcesBroker<GobblinScopeTypes> broker) {
    super (broker, HttpUtils.createApacheHttpClientLimiterKey(config));
    config = config.withFallback(FALLBACK);

    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
        .setSocketTimeout(config.getInt(REQUEST_TIME_OUT_MS_KEY))
        .setConnectTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
        .setConnectionRequestTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
        .build();

    try {
      builder.disableCookieManagement().useSystemProperties().setDefaultRequestConfig(requestConfig);
      builder.setConnectionManager(getNHttpConnManager(config));
      client = builder.build();
      client.start();
    } catch (IOException e) {
      throw new RuntimeException("ApacheHttpAsyncClient cannot be initialized");
    }
  }

  private NHttpClientConnectionManager getNHttpConnManager(Config config) throws IOException {
    NHttpClientConnectionManager httpConnManager;

    String connMgrStr = config.getString(HTTP_CONN_MANAGER);
    switch (ApacheHttpClient.ConnManager.valueOf(connMgrStr.toUpperCase())) {
      case POOLING:
        ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
        PoolingNHttpClientConnectionManager poolingConnMgr = new PoolingNHttpClientConnectionManager(ioReactor);
        poolingConnMgr.setMaxTotal(config.getInt(POOLING_CONN_MANAGER_MAX_TOTAL_CONN));
        poolingConnMgr.setDefaultMaxPerRoute(config.getInt(POOLING_CONN_MANAGER_MAX_PER_CONN));
        httpConnManager = poolingConnMgr;
        break;
      default:
        throw new IllegalArgumentException(connMgrStr + " is not supported");
    }

    LOG.info("Using " + httpConnManager.getClass().getSimpleName());
    return httpConnManager;
  }

  /**
   * A helper class which contains a latch so that we can achieve blocking calls even using
   * http async client APIs. Same can be achieved by invoking {@link Future#get()} returned by
   * {@link org.apache.http.nio.client.HttpAsyncClient#execute(HttpUriRequest, FutureCallback)}.
   * However this method seems to have a synchronization problem. It seems like {@link Future#get()}
   * is not fully blocked before callback is triggered.
   */
  @Getter
  private static class SyncHttpResponseCallback implements FutureCallback<HttpResponse> {
    private HttpUriRequest request = null;
    private HttpResponse response = null;
    private Exception exception = null;

    private final CountDownLatch latch = new CountDownLatch(1);

    public SyncHttpResponseCallback(HttpUriRequest request) {
      this.request = request;
    }

    @Override
    public void completed(HttpResponse result) {
      log.info ("Sync apache version request: {}, statusCode: {}", request, result.getStatusLine().getStatusCode());
      response = result;
      latch.countDown();
    }

    @Override
    public void failed(Exception ex) {
      exception = ex;
      latch.countDown();
    }

    @Override
    public void cancelled() {
      throw new UnsupportedOperationException("Should not be cancelled");
    }

    public void await() throws InterruptedException {
      latch.await();
    }
  }

  @Override
  public HttpResponse sendRequestImpl(HttpUriRequest request) throws IOException {
    SyncHttpResponseCallback callback = new SyncHttpResponseCallback(request);
    this.client.execute(request, callback);

    try {
      callback.await();
      if (callback.getException() != null) {
        throw new IOException(callback.getException());
      }
      return callback.getResponse();
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void sendAsyncRequestImpl(HttpUriRequest request, Callback<HttpResponse> callback) throws IOException {
    this.client.execute(request, new FutureCallback<HttpResponse>() {
      @Override
      public void completed(HttpResponse result) {
        callback.onSuccess(result);
      }

      @Override
      public void failed(Exception ex) {
        callback.onFailure(ex);
      }

      @Override
      public void cancelled() {
        throw new UnsupportedOperationException();
      }
    });
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
