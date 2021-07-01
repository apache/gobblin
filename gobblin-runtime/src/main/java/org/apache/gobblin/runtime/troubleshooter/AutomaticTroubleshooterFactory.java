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

package org.apache.gobblin.runtime.troubleshooter;

import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;


@Slf4j
public class AutomaticTroubleshooterFactory {

  private static final String TROUBLESHOOTER_CLASS = "org.apache.gobblin.troubleshooter.AutomaticTroubleshooterImpl";

  /**
   * Configures a troubleshooter that will be used inside Gobblin job or task.
   *
   * Troubleshooter will only be enabled if "gobblin-troubleshooter" modules is referenced in the application.
   * If this module is missing, troubleshooter will default to a no-op implementation.
   *
   * In addition, even when the "gobblin-troubleshooter" module is present, troubleshooter can still be disabled
   * with {@link ConfigurationKeys.TROUBLESHOOTER_DISABLED} setting.
   * */
  public static AutomaticTroubleshooter createForJob(Config config) {
    AutomaticTroubleshooterConfig troubleshooterConfig = new AutomaticTroubleshooterConfig(config);

    Class troubleshooterClass = tryGetTroubleshooterClass();

    /* Automatic troubleshooter works by intercepting log4j messages, and depends on log4j package.
     * Some users of Gobblin library have different version of log4j in their projects. If multiple versions of log4j
     * are present in a single application, it will work incorrectly. To prevent that, we moved the troubleshooter code
     * that uses log4j to a separate module. Applications that have incompatible log4j should not reference it to
     * prevent log4j problems.
     * */
    if (troubleshooterClass == null) {
      log.info("To enable Gobblin automatic troubleshooter, reference 'gobblin-troubleshooter' module in your project. "
                   + "Troubleshooter will summarize the errors and warnings in the logs. "
                   + "It will also identify root causes of certain problems.");

      return new NoopAutomaticTroubleshooter();
    } else if (troubleshooterConfig.isDisabled()) {
      log.info("Gobblin automatic troubleshooter is disabled. Remove the following property to re-enable it: "
                   + ConfigurationKeys.TROUBLESHOOTER_DISABLED);

      return new NoopAutomaticTroubleshooter();
    } else {
      InMemoryIssueRepository issueRepository = new InMemoryIssueRepository();
      DefaultIssueRefinery issueRefinery = new DefaultIssueRefinery();

      try {
        return (AutomaticTroubleshooter) GobblinConstructorUtils
            .invokeLongestConstructor(troubleshooterClass, troubleshooterConfig, issueRepository, issueRefinery);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException("Cannot create troubleshooter instance", e);
      }
    }
  }

  private static Class tryGetTroubleshooterClass() {
    try {
      return Class.forName(TROUBLESHOOTER_CLASS);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
