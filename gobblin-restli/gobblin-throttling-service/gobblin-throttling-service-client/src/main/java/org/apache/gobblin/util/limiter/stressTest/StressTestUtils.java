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

package org.apache.gobblin.util.limiter.stressTest;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Splitter;

import lombok.extern.slf4j.Slf4j;


/**
 * Utilities for throttling service stress tests.
 */
@Slf4j
public class StressTestUtils {

  public static final Option HELP_OPT = new Option("h", "Print help");
  public static final Option CONFIG_OPT = new Option("conf", true, "Set configuration for the stressor.");
  public static final Option STRESSOR_OPT = new Option("stressor", true, "Stressor class.");

  public static final String STRESSOR_CLASS = "stressTest.stressor.class";
  public static final Class<? extends Stressor> DEFAULT_STRESSOR_CLASS = FixedOperationsStressor.class;

  public static final Options OPTIONS = new Options().addOption(HELP_OPT).addOption(CONFIG_OPT);

  /**
   * Parse command line.
   */
  public static CommandLine parseCommandLine(Options options, String[] args) throws ParseException {
    CommandLineParser parser = new DefaultParser();
    CommandLine cli = parser.parse(options, args);

    if (cli.hasOption(StressTestUtils.HELP_OPT.getOpt())) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp( MRStressTest.class.getSimpleName(), OPTIONS);
      System.exit(0);
    }

    return cli;
  }

  /**
   * Add configurations provided with {@link #CONFIG_OPT} to {@link Configuration}.
   */
  public static void populateConfigFromCli(Configuration configuration, CommandLine cli) {
    String stressorClass = cli.getOptionValue(STRESSOR_OPT.getOpt(), DEFAULT_STRESSOR_CLASS.getName());
    configuration.set(STRESSOR_CLASS, stressorClass);

    if (cli.hasOption(CONFIG_OPT.getOpt())) {
      for (String arg : cli.getOptionValues(CONFIG_OPT.getOpt())) {
        List<String> tokens = Splitter.on(":").limit(2).splitToList(arg);
        if (tokens.size() < 2) {
          throw new IllegalArgumentException("Configurations must be of the form <key>:<value>");
        }
        configuration.set(tokens.get(0), tokens.get(1));
      }
    }
  }
}
