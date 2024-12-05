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

import java.util.Optional;

import org.apache.hadoop.fs.FileSystem;

import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.dynamic.FsScalingDirectiveSource;
import org.apache.gobblin.temporal.dynamic.ScalingDirectiveSource;

/**
 * {@link FsScalingDirectiveSource} based implementation of {@link AbstractDynamicScalingYarnServiceManager}.
 */
public class FsSourceDynamicScalingYarnServiceManager extends AbstractDynamicScalingYarnServiceManager {
  public final static String DYNAMIC_SCALING_DIRECTIVES_DIR = GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_PREFIX + "directives.dir";
  public final static String DYNAMIC_SCALING_ERRORS_DIR = GobblinTemporalConfigurationKeys.DYNAMIC_SCALING_PREFIX + "errors.dir";
  private final FileSystem fs;

  public FsSourceDynamicScalingYarnServiceManager(GobblinTemporalApplicationMaster appMaster) {
    super(appMaster);
    this.fs = appMaster.getFs();
  }

  @Override
  protected ScalingDirectiveSource createScalingDirectiveSource() {
    return new FsScalingDirectiveSource(
        this.fs,
        this.config.getString(DYNAMIC_SCALING_DIRECTIVES_DIR),
        Optional.ofNullable(this.config.getString(DYNAMIC_SCALING_ERRORS_DIR))
    );
  }
}
