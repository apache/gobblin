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

package org.apache.gobblin.cluster;

import java.io.PrintWriter;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;


class SingleTaskRunnerMainOptions {
  private static final Logger logger = LoggerFactory.getLogger(SingleTaskRunnerMainOptions.class);
  static final String CLUSTER_CONFIG_FILE_PATH = "cluster_config_file_path";
  static final String WORK_UNIT_FILE_PATH = "work_unit_file_path";
  static final String JOB_ID = "job_id";
  private static final ImmutableMap<String, String> OPTIONS_MAP = ImmutableMap
      .of(JOB_ID, "job id", WORK_UNIT_FILE_PATH, "work unit file path", CLUSTER_CONFIG_FILE_PATH,
          "cluster configuration file path");
  private static final int CHARACTERS_PER_LINE = 80;

  private final PrintWriter writer;
  private CommandLine cmd;
  private Options options;

  SingleTaskRunnerMainOptions(final String[] args, final PrintWriter writer) {
    this.writer = writer;
    initCmdLineOptions(args);
  }

  private void initCmdLineOptions(final String[] args) {
    this.options = buildExpectedOptions();
    try {
      this.cmd = new DefaultParser().parse(this.options, args);
    } catch (final ParseException e) {
      logger.error("failed to parse command options.", e);
      printUsage(this.options);
      throw new GobblinClusterException("Failed to parse command line options", e);
    }
  }

  private Options buildExpectedOptions() {
    final Options options = new Options();
    for (final Map.Entry<String, String> entry : OPTIONS_MAP.entrySet()) {
      final Option option =
          Option.builder(null).required(true).longOpt(entry.getKey()).desc(entry.getValue()).hasArg().build();
      options.addOption(option);
    }
    return options;
  }

  private void printUsage(final Options options) {
    final HelpFormatter formatter = new HelpFormatter();
    formatter.printUsage(this.writer, CHARACTERS_PER_LINE, SingleTaskRunnerMain.class.getSimpleName(), options);
  }

  String getJobId() {
    return this.cmd.getOptionValue(JOB_ID);
  }

  String getWorkUnitFilePath() {
    return this.cmd.getOptionValue(WORK_UNIT_FILE_PATH);
  }

  String getClusterConfigFilePath() {
    return this.cmd.getOptionValue(CLUSTER_CONFIG_FILE_PATH);
  }
}
