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

package org.apache.gobblin.runtime.retention;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.data.management.retention.DatasetCleaner;
import org.apache.gobblin.runtime.cli.CliApplication;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;


@Alias(value = "cleaner", description = "Data retention utility")
public class DatasetCleanerCli implements CliApplication {
  private static final Option CLEANER_CONFIG =
          Option.builder("c").longOpt("config").hasArg().required().desc("DatasetCleaner configuration").build();

  @Override
  public void run(String[] args) {
    try {
      Properties properties = readProperties(parseConfigLocation(args));
      DatasetCleaner datasetCleaner = new DatasetCleaner(FileSystem.get(new Configuration()), properties);
      datasetCleaner.clean();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Properties readProperties(String fileLocation) {
    try {
      Properties prop = new Properties();
      FileInputStream input = new FileInputStream(fileLocation);
      prop.load(input);
      input.close();
      return prop;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String parseConfigLocation(String[] args) {
    Options options = new Options();
    options.addOption(CLEANER_CONFIG);

    CommandLine cli;
    try {
      CommandLineParser parser = new DefaultParser();
      cli = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
    } catch (ParseException pe) {
      System.out.println("Command line parse exception: " + pe.getMessage());
      printUsage(options);
      throw new RuntimeException(pe);
    }
    return cli.getOptionValue(CLEANER_CONFIG.getOpt());
  }

  private void printUsage(Options options) {
    HelpFormatter formatter = new HelpFormatter();

    String usage = "DatasetCleaner configuration ";
    formatter.printHelp(usage, options);
  }
}
