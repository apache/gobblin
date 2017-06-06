package gobblin.http;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.codahale.metrics.Timer;
import com.typesafe.config.Config;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.metrics.MetricContext;
import gobblin.metrics.broker.MetricContextFactory;
import gobblin.metrics.broker.MetricContextKey;
import gobblin.util.http.HttpLimiterKey;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.broker.SharedLimiterFactory;


/**
 * A {@link HttpClient} for throttling calls to the underlying TX operation using the input
 * {@link Limiter}.
 */
@Slf4j
public abstract class ThrottledHttpClient<RQ, RP> implements HttpClient<RQ, RP>  {

  protected final Limiter limiter;
  protected final SharedResourcesBroker<GobblinScopeTypes> broker;
  protected final Config config;

  @Getter
  private final Timer sendTimer;
  private final MetricContext metricContext;

  public ThrottledHttpClient (Config config, SharedResourcesBroker<GobblinScopeTypes> broker) {
    this.config = config;
    this.broker = broker;
    try {
      this.limiter = broker.getSharedResource(new SharedLimiterFactory<>(), new HttpLimiterKey(getLimiterKey ()));
      this.metricContext = broker.getSharedResource(new MetricContextFactory<>(), new MetricContextKey());
      this.sendTimer = this.metricContext.timer(getLimiterKey());
    } catch (NotConfiguredException e) {
      log.error ("Limiter cannot be initialized due to exception " + ExceptionUtils.getFullStackTrace(e));
      throw new RuntimeException(e);
    }
  }

  public RP sendRequest(RQ request) throws IOException {
    final Timer.Context context = sendTimer.time();
    try {
      if (limiter.acquirePermits(1) != null) {
        log.debug ("Acquired permits successfully");
        return sendRequestImpl (request);
      } else {
        throw new IOException ("Acquired permits return null");
      }
    } catch (InterruptedException e) {
      throw new IOException("Throttling is interrupted");
    } finally {
      context.stop();
    }
  }

  public abstract RP sendRequestImpl (RQ request) throws IOException;

  protected abstract String getLimiterKey () throws NotConfiguredException;
}
