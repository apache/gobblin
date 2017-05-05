package gobblin.http;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.Getter;
import lombok.extern.log4j.Log4j;

import gobblin.configuration.State;
import gobblin.writer.http.AbstractHttpWriterBuilder;
import gobblin.writer.http.DelegatingHttpClientConnectionManager;


/**
 * A {@link HttpClient} that sends {@link HttpUriRequest} and gets {@link CloseableHttpResponse}.
 * It encapsulates a {@link CloseableHttpClient} instance to send the {@link HttpUriRequest}
 */
@Log4j
public class ApacheHttpClient implements HttpClient<HttpUriRequest, CloseableHttpResponse> {
  public static final String HTTP_CONN_MANAGER = "conn_mgr_type";
  public static final String POOLING_CONN_MANAGER_MAX_TOTAL_CONN = "conn_mgr.pooling.max_conn_total";
  public static final String POOLING_CONN_MANAGER_MAX_PER_CONN = "conn_mgr.pooling.max_per_conn";
  public static final String REQUEST_TIME_OUT_MS_KEY = "req_time_out";
  public static final String CONNECTION_TIME_OUT_MS_KEY = "conn_time_out";
  public static final String STATIC_SVC_ENDPOINT = "static_svc_endpoint";

  public enum ConnManager {
    POOLING,
    BASIC
  }

  private static final Config FALLBACK =
      ConfigFactory.parseMap(ImmutableMap.<String, Object>builder()
          .put(REQUEST_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(5L))
          .put(CONNECTION_TIME_OUT_MS_KEY, TimeUnit.SECONDS.toMillis(5L))
          .put(HTTP_CONN_MANAGER, AbstractHttpWriterBuilder.ConnManager.BASIC.name())
          .put(POOLING_CONN_MANAGER_MAX_TOTAL_CONN, 20)
          .put(POOLING_CONN_MANAGER_MAX_PER_CONN, 2)
          .build());

  /**
   * A helper class to customize and track http connection
   */
  class HttpClientConnectionManagerWithConnTracking extends DelegatingHttpClientConnectionManager {
    HttpClientConnectionManagerWithConnTracking(HttpClientConnectionManager fallback) {
      super(fallback);
    }

    @Override
    public ConnectionRequest requestConnection(HttpRoute route, Object state) {
      try {
        ApacheHttpClient.this.connect(new URI(route.getTargetHost().toURI()));
      } catch (IOException | URISyntaxException e) {
        throw new RuntimeException("onConnect() callback failure: " + e, e);
      }
      return super.requestConnection(route, state);
    }
  }

  private final CloseableHttpClient client;
  @Getter
  protected URI serverHost;

  public ApacheHttpClient(HttpClientBuilder builder, Config config) {
    config = config.withFallback(FALLBACK);
    RequestConfig requestConfig = RequestConfig.copy(RequestConfig.DEFAULT)
        .setSocketTimeout(config.getInt(REQUEST_TIME_OUT_MS_KEY))
        .setConnectTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
        .setConnectionRequestTimeout(config.getInt(CONNECTION_TIME_OUT_MS_KEY))
        .build();

    builder.disableCookieManagement().useSystemProperties().setDefaultRequestConfig(requestConfig);
    builder.setConnectionManager(new HttpClientConnectionManagerWithConnTracking(getHttpConnManager(config)));
    client = builder.build();
  }

  public boolean connect(URI uri) throws IOException {
    return false;
  }

  @Override
  public CloseableHttpResponse sendRequest(HttpUriRequest request) throws IOException {
    return client.execute(request);
  }

  private HttpClientConnectionManager getHttpConnManager(Config config) {
    HttpClientConnectionManager httpConnManager;

    if (config.hasPath(STATIC_SVC_ENDPOINT)) {
      try {
        serverHost = new URI(config.getString(STATIC_SVC_ENDPOINT));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

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

    log.info("Using " + httpConnManager.getClass().getSimpleName());
    return httpConnManager;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
