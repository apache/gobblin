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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


/**
 * An extension of {@link AzkabanGobblinYarnAppLauncher} for locally-running Azkaban instances since it provides
 * capability of changing yarn-resource related configuration in the way that could work with lighter hardware.
 */
public class AzkabanGobblinLocalYarnAppLauncher extends AzkabanGobblinYarnAppLauncher {
  public AzkabanGobblinLocalYarnAppLauncher(String jobId, Properties gobblinProps)
      throws IOException {
    super(jobId, gobblinProps);
  }

  @Override
  protected YarnConfiguration initYarnConf(Properties gobblinProps) {
    YarnConfiguration yarnConfiguration = super.initYarnConf(gobblinProps);
    if (gobblinProps.containsKey("yarn-site-address")) {
      yarnConfiguration.addResource(new Path(gobblinProps.getProperty("yarn-site-address")));
    } else {
      yarnConfiguration.set("yarn.resourcemanager.connect.max-wait.ms", "10000");
      yarnConfiguration.set("yarn.nodemanager.resource.memory-mb", "512");
      yarnConfiguration.set("yarn.scheduler.maximum-allocation-mb", "1024");
    }
    return yarnConfiguration;
  }
}
