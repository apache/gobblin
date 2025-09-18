package org.apache.gobblin.service.monitoring;

import com.google.common.util.concurrent.Service;


/**
 * A marker interface for spec change monitors to generalize initialization in {@link org.apache.gobblin.service.modules.core.GobblinServiceManager#specStoreChangeMonitor}
 */
public interface SpecChangeMonitor extends Service {
  void setActive();
}
