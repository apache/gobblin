package org.apache.gobblin.service.monitoring;

import com.google.common.util.concurrent.Service;


/**
 * A marker interface for dag action change monitors to generalize initialization in {@link org.apache.gobblin.service.modules.core.GobblinServiceManager#dagManagementDagActionStoreChangeMonitor}
 */
public interface DagActionChangeMonitor extends Service {
  void setActive();
}
