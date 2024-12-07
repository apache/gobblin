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

package org.apache.gobblin.temporal.yarn;

import org.apache.gobblin.temporal.dynamic.DummyScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;

/**
 * {@link DummyScalingDirectiveSource} based implementation of {@link AbstractDynamicScalingYarnServiceManager}.
 *  This class is meant to be used for integration testing purposes only.
 *  This is initialized using config {@link org.apache.gobblin.yarn.GobblinYarnConfigurationKeys#APP_MASTER_SERVICE_CLASSES} while testing
 */
public class DummyDynamicScalingYarnServiceManager extends AbstractDynamicScalingYarnServiceManager {

  public DummyDynamicScalingYarnServiceManager(GobblinTemporalApplicationMaster appMaster) {
    super(appMaster);
  }

  @Override
  protected ScalingDirectiveSource createScalingDirectiveSource() {
    return new DummyScalingDirectiveSource();
  }
}