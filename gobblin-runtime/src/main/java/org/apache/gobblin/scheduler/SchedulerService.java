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

package gobblin.scheduler;

import java.util.Properties;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractIdleService;
import com.typesafe.config.Config;

import gobblin.configuration.ConfigurationKeys;
import gobblin.util.ConfigUtils;
import gobblin.util.PropertiesUtils;

import lombok.Getter;


/**
 * A {@link com.google.common.util.concurrent.Service} wrapping a Quartz {@link Scheduler} allowing correct shutdown
 * of the scheduler when {@link JobScheduler} fails to initialize.
 */
public class SchedulerService extends AbstractIdleService {

  @Getter
  private Scheduler scheduler;
  private final boolean waitForJobCompletion;
  private final Optional<Properties> quartzProps;

  public SchedulerService(boolean waitForJobCompletion, Optional<Properties> quartzConfig) {
    this.waitForJobCompletion = waitForJobCompletion;
    this.quartzProps = quartzConfig;
  }

  public SchedulerService(Properties props) {
    this(Boolean.parseBoolean(
            props.getProperty(ConfigurationKeys.SCHEDULER_WAIT_FOR_JOB_COMPLETION_KEY,
                              ConfigurationKeys.DEFAULT_SCHEDULER_WAIT_FOR_JOB_COMPLETION)),
        Optional.of(PropertiesUtils.extractPropertiesWithPrefix(props, Optional.of("org.quartz."))));
  }

  public SchedulerService(Config cfg) {
    this(cfg.hasPath(ConfigurationKeys.SCHEDULER_WAIT_FOR_JOB_COMPLETION_KEY) ?
         cfg.getBoolean(ConfigurationKeys.SCHEDULER_WAIT_FOR_JOB_COMPLETION_KEY) :
         Boolean.parseBoolean(ConfigurationKeys.DEFAULT_SCHEDULER_WAIT_FOR_JOB_COMPLETION),
         Optional.of(ConfigUtils.configToProperties(cfg, "org.quartz.")));
  }

  @Override protected void startUp() throws SchedulerException  {
    StdSchedulerFactory schedulerFactory = new StdSchedulerFactory();
    if (this.quartzProps.isPresent() && this.quartzProps.get().size() > 0) {
      schedulerFactory.initialize(this.quartzProps.get());
    }
    this.scheduler = schedulerFactory.getScheduler();
    this.scheduler.start();
  }

  @Override protected void shutDown() throws SchedulerException  {
    this.scheduler.shutdown(this.waitForJobCompletion);
  }
}
