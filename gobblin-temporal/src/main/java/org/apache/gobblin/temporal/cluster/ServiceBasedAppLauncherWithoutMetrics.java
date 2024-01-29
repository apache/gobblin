package org.apache.gobblin.temporal.cluster;

import java.util.Properties;

import org.apache.gobblin.runtime.app.ServiceBasedAppLauncher;


/**
 * {@link ServiceBasedAppLauncher} that does not add metrics service. This is different from deactivating the flag
 * {@link org.apache.gobblin.configuration.ConfigurationKeys#METRICS_ENABLED_KEY} because this will only not initialize
 * the metrics service in the cluster manager but still allow for {@link org.apache.gobblin.metrics.GobblinTrackingEvent}
 */
public class ServiceBasedAppLauncherWithoutMetrics extends ServiceBasedAppLauncher {
  public ServiceBasedAppLauncherWithoutMetrics(Properties properties, String appName)
      throws Exception {
    super(properties, appName);
  }

  @Override
  protected void addMetricsService(Properties properties) {
  }
}
