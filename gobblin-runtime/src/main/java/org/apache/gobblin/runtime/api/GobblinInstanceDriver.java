/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.runtime.api;

import java.util.concurrent.Executor;

import com.codahale.metrics.Gauge;
import com.google.common.util.concurrent.Service;

import gobblin.annotation.Alpha;
import gobblin.metrics.ContextAwareCounter;
import gobblin.metrics.ContextAwareGauge;

import lombok.Getter;

/**
 * An interface that defines a Gobblin Instance driver which knows how to monitor for Gobblin
 * {@link JobSpec}s and run them.
 * */
@Alpha
public interface GobblinInstanceDriver extends Service, JobLifecycleListenersContainer,
                                               GobblinInstanceEnvironment {

  /** The service that keeps track of jobs that are known to Gobblin */
  JobCatalog getJobCatalog();

  /**
   * Returns a mutable instance of the job catalog
   * (if it implements the {@link MutableJobCatalog} interface). Implementation will throw
   * ClassCastException if the current catalog is not mutable.
   */
  MutableJobCatalog getMutableJobCatalog();

  /** The service the determine when jobs should be executed.*/
  JobSpecScheduler getJobScheduler();

  /** The service for executing Gobblin jobs */
  JobExecutionLauncher getJobLauncher();

  /** Metrics for instance */
  StandardMetrics getMetrics();

  public class StandardMetrics extends Service.Listener {
    public static final String INSTANCE_NAME_TAG = "instanceName";
    public static final String UPTIMEMS_NAME = "uptimeMs";
    public static final String UPFLAG_NAME = "upFlag";
    public static final String NUM_UNCLASSIFIED_ERRORS_NAME = "numUnclassifiedErrors";

    /** The time in milliseconds since the instance started or 0 if not running. */
    @Getter private final ContextAwareGauge<Long> uptimeMs;
    /** 1 if running, -1 if stopped due to error, 0 if not started or shutdown */
    @Getter private final ContextAwareGauge<Integer> upFlag;
    /** Total error count detected by this instance which have not been classified and tracked in
     * a dedicated counter. */
    @Getter private final ContextAwareCounter numUnclassifiedErrors;
    private long startTimeMs;

    public StandardMetrics(final GobblinInstanceDriver parent) {
      this.uptimeMs = parent.getMetricContext().newContextAwareGauge(UPTIMEMS_NAME, new Gauge<Long>() {
        @Override public Long getValue() {
          return startTimeMs > 0 ? System.currentTimeMillis() - startTimeMs : 0;
        }
      });
      this.upFlag = parent.getMetricContext().newContextAwareGauge(UPFLAG_NAME, new Gauge<Integer>() {
        @Override public Integer getValue() {
          switch (parent.state()) {
            case RUNNING: return 1;
            case FAILED: return -1;
            default: return 0;
          }
        }
      });
      this.numUnclassifiedErrors = parent.getMetricContext().contextAwareCounter(NUM_UNCLASSIFIED_ERRORS_NAME);

      parent.addListener(this, new Executor() {
        @Override public void execute(Runnable command) {
          command.run();
        }
      });
    }

    @Override public void running() {
      this.startTimeMs = System.currentTimeMillis();
    }

    @Override public void terminated(State from) {
      this.startTimeMs = 0;
    }

    @Override public void failed(State from, Throwable failure) {
      this.startTimeMs = 0;
    }
  }
}
