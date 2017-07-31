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
package org.apache.gobblin.data.management.retention.action;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import org.apache.gobblin.data.management.policy.VersionSelectionPolicy;
import org.apache.gobblin.data.management.retention.dataset.ConfigurableCleanableDataset;
import org.apache.gobblin.data.management.version.DatasetVersion;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A wrapper around {@link AccessControlAction} that delegates the {@link #execute(List)} call to multiple embedded
 * {@link AccessControlAction}s.
 * <p>
 * The embedded {@link AccessControlAction}s are specified at key <code>policies</code> of the <code>actionConfig</code>.
 * If {@link VersionSelectionPolicy}s of different embedded {@link AccessControlAction}s overlap, the last {@link AccessControlAction}
 * in <code>policies</code> key wins.
 * </p>
 * Use {@link MultiAccessControlActionFactory} to create new {@link MultiAccessControlAction}s
 */
public class MultiAccessControlAction extends RetentionAction {

  private final List<AccessControlAction> embeddedAccessControlActions;
  private static final String POLICIES_KEY = "policies";

  /**
   * The expected paths in the <code>actionConfig</code> are shown below.
   * <pre>
   * {
   *      ## list all the policies, each policy should have a path in the config
   *      policies = [restricted, .....]
   *
   *       restricted {
   *           selection {
   *             policy.class=org.apache.gobblin.data.management.policy.SelectBeforeTimeBasedPolicy
   *            timeBased.lookbackTime = 7d
   *         }
   *          mode : 750
   *          owner : onr
   *          group : grp
   *      }
   *   }
   * </pre>
   * @param actionConfig to use while creating a new {@link MultiAccessControlAction}
   * @param fs
   */
  private MultiAccessControlAction(Config actionConfig, FileSystem fs, Config jobConfig) {
    super(actionConfig, fs, jobConfig);
    this.embeddedAccessControlActions = Lists.newArrayList();
    for (String policy : ConfigUtils.getStringList(actionConfig, POLICIES_KEY)) {
      Preconditions.checkArgument(
          actionConfig.hasPath(policy),
          String.format("Policy %s is specified at key %s but actionConfig does not have config for this policy."
              + "Complete actionConfig %s", policy, POLICIES_KEY,
              actionConfig.root().render(ConfigRenderOptions.concise())));
      embeddedAccessControlActions.add(new AccessControlAction(actionConfig.getConfig(policy), fs, jobConfig));
    }
  }

  /**
   * Calls {@link AccessControlAction#execute(List)} on each of the embedded {@link AccessControlAction}s
   *
   * {@inheritDoc}
   * @see org.apache.gobblin.data.management.retention.action.RetentionAction#execute(java.util.List)
   */
  @Override
  public void execute(List<DatasetVersion> allVersions) throws IOException {
    for (AccessControlAction aca : this.embeddedAccessControlActions) {
      aca.execute(allVersions);
    }
  }

  /**
   * A factory class to create {@link MultiAccessControlAction}s
   */
  public static class MultiAccessControlActionFactory implements RetentionActionFactory {

    private static String ACCESS_CONTROL_KEY = "accessControl";
    private static String LEGACY_ACCESS_CONTROL_KEY = ConfigurableCleanableDataset.RETENTION_CONFIGURATION_KEY + "."
        + ACCESS_CONTROL_KEY;

    @Override
    public MultiAccessControlAction createRetentionAction(Config config, FileSystem fs, Config jobConfig) {
      Preconditions.checkArgument(this.canCreateWithConfig(config),
          "Can not create MultiAccessControlAction with config " + config.root().render(ConfigRenderOptions.concise()));
      if (config.hasPath(LEGACY_ACCESS_CONTROL_KEY)) {
        return new MultiAccessControlAction(config.getConfig(LEGACY_ACCESS_CONTROL_KEY), fs, jobConfig);
      } else if (config.hasPath(ACCESS_CONTROL_KEY)) {
        return new MultiAccessControlAction(config.getConfig(ACCESS_CONTROL_KEY), fs, jobConfig);
      }
      throw new IllegalStateException(
          "RetentionActionFactory.canCreateWithConfig returned true but could not create MultiAccessControlAction");
    }

    @Override
    public boolean canCreateWithConfig(Config config) {
      return config.hasPath(LEGACY_ACCESS_CONTROL_KEY) || config.hasPath(ACCESS_CONTROL_KEY);
    }
  }
}
