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

package org.apache.gobblin.runtime.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class MigrationCliOptions {
  public final static Option CONFIG_OPTION = Option.builder("c")
      .argName("Configuration file")
      .desc("state migration configuration file")
      .hasArgs()
      .longOpt("config")
      .build();

  public final static Option HELP_OPTION =
      Option.builder("h").argName("help").desc("Display usage information").longOpt("help").build();

  /**
   * Parse command line arguments and return a {@link Config} for migration job.
   */
  public static Config parseArgs(Class<?> caller, String[] args) throws IOException {
    try {
      // Parse command-line options
      CommandLine cmd = new DefaultParser().parse(options(), args);

      if (cmd.hasOption(HELP_OPTION.getOpt())) {
        printUsage(caller);
        System.exit(0);
      }

      if (!cmd.hasOption(CONFIG_OPTION.getOpt())) {
        printUsage(caller);
        System.exit(1);
      }

      FileSystem fs = FileSystem.get(new Configuration());
      FSDataInputStream inputStream = fs.open(new Path(cmd.getOptionValue(CONFIG_OPTION.getLongOpt())));
      return ConfigFactory.parseReader(new InputStreamReader(inputStream));
    } catch (ParseException e) {
      throw new IOException(e);
    }
  }

  /**
   * Prints the usage of cli.
   * @param caller Class of the main method called. Used in printing the usage message.
   */
  public static void printUsage(Class<?> caller) {
    new HelpFormatter().printHelp(caller.getSimpleName(), options());
  }

  private static Options options() {
    Options options = new Options();
    options.addOption(CONFIG_OPTION);
    options.addOption(HELP_OPTION);
    return options;
  }
}
