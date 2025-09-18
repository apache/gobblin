package org.apache.gobblin.service.monitoring;

import com.google.common.util.concurrent.Service;


/**
 * A marker interface for job status monitors to generalize initialization in {@link org.apache.gobblin.service.modules.core.GobblinServiceManager#jobStatusMonitor}
 */
public interface JobStatusMonitor extends Service {
}
