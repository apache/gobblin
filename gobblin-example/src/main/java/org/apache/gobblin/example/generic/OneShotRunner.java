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

package org.apache.gobblin.example.generic;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.runtime.api.JobTemplate;
import org.apache.gobblin.runtime.cli.CliObjectOption;
import org.apache.gobblin.runtime.cli.PublicMethodsGobblinCliFactory;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblin;
import org.apache.gobblin.util.JobConfigurationUtils;


/**
 * A class that allows Gobblin cli to run a single pull file (in a blocking manner) and exit on completion
 * In contrast, a Gobblin standalone service will run forever polling
 * for new pull files in the configured directory
 */
@Slf4j
public class OneShotRunner extends EmbeddedGobblin {
  @Alias(value = "oneShot", description = "Gobblin command that runs one pull file in standalone or map-reduce mode")
  public static class CliFactory extends PublicMethodsGobblinCliFactory {


    public CliFactory() {
      super(OneShotRunner.class);
    }

    @Override
    public EmbeddedGobblin constructEmbeddedGobblin(CommandLine cli) throws JobTemplate.TemplateException, IOException {
      String[] leftoverArgs = cli.getArgs();
      if (leftoverArgs.length != 0) {
        throw new RuntimeException("Unexpected number of arguments.");
      }
      return new OneShotRunner();
    }

    @Override
    public String getUsageString() {
      return "[OPTIONS]";
    }
  }

  @SneakyThrows
  @CliObjectOption(description = "Runs the job in MR mode")
  public OneShotRunner mrMode() {
    super.mrMode();
    return this;
  }

  @CliObjectOption(description = "Sets the base configuration file")
  public OneShotRunner baseConf(String baseConfFile) {
    log.info("Configured with baseConf file = {}", baseConfFile);
    try {
      Properties sysConfig = JobConfigurationUtils.fileToProperties(baseConfFile);
      log.debug("Loaded up base config: {}", sysConfig);
      sysConfig.entrySet()
          .stream()
          .forEach(pair -> super.sysConfig(pair.getKey().toString(), pair.getValue().toString()));

    } catch (Exception e) {
      throw new RuntimeException("Failed to load configuration from base config file : " + baseConfFile, e);
    }
    return this;
  }

  @CliObjectOption(description = "Sets the application configuration file")
  public OneShotRunner appConf(String appConfFile) {
    super.jobFile(appConfFile);
    return this;
  }

  public OneShotRunner() {
    super("Generic");
  }

}
