package gobblin.http;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.async.Callback;
import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.utils.HttpUtils;


/**
 * A synchronous {@link HttpClient} that sends {@link HttpUriRequest} and gets {@link CloseableHttpResponse}.
 * It encapsulates a {@link CloseableHttpClient} instance to send the {@link HttpUriRequest}
 *
 * {@link CloseableHttpClient} is used
 */
@Slf4j
public class ApacheHttpClient extends ThrottledHttpClient<HttpUriRequest, CloseableHttpResponse> {
  private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpClient.class);

  public static final String HTTP_CONN_MANAGER = "connMgrType";
  public static final String POOLING_CONN_MANAGER_MAX_TOTAL_CONN = "connMgr.pooling.maxTotalConn";
  public static final String POOLING_CONN_MANAGER_MAX_PER_CONN = "connMgr.pooling.maxPerConn";
  public static final String REQUEST_TIME_OUT_MS_KEY = "reqTimeout";
  public static final String CONNECTION_TIME_OUT_MS_KEY = "connTimeout";

  public enum ConnManager {
    POOLING,
    BASIC
  }

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(REQUEST_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(10L))
          .put(CONNECTION_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(10L))
          .put(HTTP_CONN_MANAGER, ConnManager.BASIC.name())
          .put(POOLING_CONN_MANAGER_MAX_TOTAL_CONN, 20)
          .put(POOLING_CONN_MANAGER_MAX_PER_CONN, 2)
          .build());

  private final CloseableHttpClient client;
  public ApacheHttpClient(HttpClientBuilder builder, Config config, SharedResourcesBroker<GobblinScopeTypes> broker) {
    super (broker, HttpUtils.createApacheHttpClientLimiterKey(config));
    config = config.withFallback(FALLBACK);

    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
        .setSocketTimeout(config.getInt(REQUEST_TIME_OUT_MS_KEY))
        .setConnectTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
        .setConnectionRequestTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
        .build();

    builder.disableCookieManagement().useSystemProperties().setDefaultRequestConfig(requestConfig);
    builder.setConnectionManager(getHttpConnManager(config));
    client = builder.build();
  }

  @Override
  public CloseableHttpResponse sendRequestImpl(HttpUriRequest request) throws IOException {
    return client.execute(request);
  }

  @Override
  public void sendAsyncRequestImpl(HttpUriRequest request, Callback<CloseableHttpResponse> callback) throws IOException {
    throw new UnsupportedOperationException("ApacheHttpClient doesn't support asynchronous send");
  }

  private HttpClientConnectionManager getHttpConnManager(Config config) {
    HttpClientConnectionManager httpConnManager;

    String connMgrStr = config.getString(HTTP_CONN_MANAGER);
    switch (ConnManager.valueOf(connMgrStr.toUpperCase())) {
      case BASIC:
        httpConnManager = new BasicHttpClientConnectionManager();
        break;
      case POOLING:
        PoolingHttpClientConnectionManager poolingConnMgr = new PoolingHttpClientConnectionManager();
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

  @Override
  public void close() throws IOException {
    client.close();
  }
}
