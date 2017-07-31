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

package org.apache.gobblin.azkaban;

import java.util.List;
import java.util.Properties;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import org.apache.gobblin.metrics.RootMetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.scheduler.SchedulerDaemon;
import azkaban.jobExecutor.AbstractJob;


/**
 * Wrapper of {@link SchedulerDaemon}, specially used by Azkaban to launch a job scheduler daemon.
 */

public class AzkabanGobblinDaemon extends AbstractJob {

  private static final Logger LOG = Logger.getLogger(AzkabanGobblinDaemon.class);

  private SchedulerDaemon daemon;

  public AzkabanGobblinDaemon(String jobId, Properties props) throws Exception {
    super(jobId, LOG);

    List<Tag<?>> tags = Lists.newArrayList();
    tags.addAll(Tag.fromMap(AzkabanTags.getAzkabanTags()));
    RootMetricContext.get(tags);

    this.daemon = new SchedulerDaemon(props);
  }

  @Override
  public void run()
      throws Exception {
    this.daemon.start();
  }

  @Override
  public void cancel()
      throws Exception {
    this.daemon.stop();
  }
}
