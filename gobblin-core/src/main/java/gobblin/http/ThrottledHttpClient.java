package gobblin.http;

import java.io.IOException;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
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

  public ThrottledHttpClient (Config config, SharedResourcesBroker<GobblinScopeTypes> broker) {
    this.config = config;
    this.broker = broker;
    try {
      limiter = broker.getSharedResource(new SharedLimiterFactory<>(), new HttpLimiterKey(getLimiterKey ()));
    } catch (NotConfiguredException e) {
      log.error ("Limiter cannot be initialized due to exception " + ExceptionUtils.getFullStackTrace(e));
      throw new RuntimeException(e);
    }
  }

  public RP sendRequest(RQ request) throws IOException {
    try {
      if (limiter.acquirePermits(1) != null) {
        log.debug ("Acquired permits successfully");
        return sendRequestImpl (request);
      } else {
        throw new IOException ("Acquired permits return null");
      }
    } catch (InterruptedException e) {
      throw new IOException("Throttling is interrupted");
    }
  }

  public abstract RP sendRequestImpl (RQ request) throws IOException;

  protected abstract String getLimiterKey () throws NotConfiguredException;
}
