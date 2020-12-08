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

package org.apache.gobblin.runtime;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Throwables;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.annotation.Alias;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.runtime.cli.CliApplication;


/**
 * A Cli to inspect streaming watermarks.
 */
@Alias(value = "watermarks", description = "Inspect streaming watermarks")
@Slf4j
public class StateStoreBasedWatermarkStorageCli implements CliApplication {

  private static final Option HELP = Option.builder("h").longOpt("help").build();
  private static final Option ZK = Option.builder("z").longOpt("zk")
      .desc("Zk connect string").hasArg().build();
  private static final Option JOB_NAME = Option.builder("j").longOpt("jobName")
      .desc("The Job name").hasArg().build();
  private static final Option ROOT_DIR = Option.builder("r").longOpt("rootDir")
      .desc("The State Store Root Directory").hasArg().build();
  private static final Option WATCH = Option.builder("w").longOpt("watch")
      .desc("Watch the watermarks").build();
  private static final Option DELETE = Option.builder("d").longOpt("delete")
      .desc("Delete the watermarks associated with a job").build();

  private CommandLine cli;

  @Override
  public void run(String[] args) {
    this.cli = initializeAndVerifyOptions(args);

    // Option is missing or help option called
    if (this.cli == null) {
      return;
    }

    TaskState taskState = new TaskState();

    String jobName = this.cli.getOptionValue(JOB_NAME.getOpt());
    log.info("Using job name: {}", jobName);
    taskState.setProp(ConfigurationKeys.JOB_NAME_KEY, jobName);

    String zkAddress = "locahost:2181";
    if (cli.hasOption(ZK.getOpt())) {
      zkAddress = cli.getOptionValue(ZK.getOpt());
    }

    log.info("Using zk address : {}", zkAddress);

    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_TYPE_KEY, "zk");
    taskState.setProp("state.store.zk.connectString", zkAddress);

    String rootDir = cli.getOptionValue(ROOT_DIR.getOpt());
    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_CONFIG_PREFIX
        + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, rootDir);
    log.info("Setting root dir to {}", rootDir);

    StateStoreBasedWatermarkStorage stateStoreBasedWatermarkStorage = new StateStoreBasedWatermarkStorage(taskState);

    if (cli.hasOption(DELETE.getOpt())) {
      try {
        stateStoreBasedWatermarkStorage.removeAllJobWatermark();
      } catch (IOException e) {
        System.out.println("Job " + jobName + " not found in state store");
        return;
      }
    }

    this.displayWatermarks(stateStoreBasedWatermarkStorage);
  }

  private CommandLine initializeAndVerifyOptions(String[] args) {
    Options options = new Options();
    options.addOption(HELP);
    options.addOption(ZK);
    options.addOption(JOB_NAME);
    options.addOption(ROOT_DIR);
    options.addOption(WATCH);
    options.addOption(DELETE);

    CommandLine commandLine;
    try {
      CommandLineParser parser = new DefaultParser();
      commandLine = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
    } catch (ParseException pe) {
      System.out.println( "Command line parse exception: " + pe.getMessage() );
      throw new RuntimeException(pe);
    }

    if (commandLine.hasOption(HELP.getOpt())) {
      printUsage(options);
      return null;
    }

    if (!commandLine.hasOption(JOB_NAME.getOpt())) {
      System.out.println("Need Job Name to be specified --" + JOB_NAME.getLongOpt());
      printUsage(options);
      return null;
    }

    if (!commandLine.hasOption(ROOT_DIR.getOpt())) {
      System.out.println("Need root directory specified");
      printUsage(options);
      return null;
    }

    return commandLine;
  }



  private void printUsage(Options options) {

    HelpFormatter formatter = new HelpFormatter();
    formatter.setOptionComparator(new Comparator<Option>() {
      @Override
      public int compare(Option o1, Option o2) {
        if (o1.isRequired() && !o2.isRequired()) {
          return -1;
        }
        if (!o1.isRequired() && o2.isRequired()) {
          return 1;
        }
        return o1.getOpt().compareTo(o2.getOpt());
      }
    });

    String usage = "gobblin watermarks ";
    formatter.printHelp(usage, options);
  }

  private void displayWatermarks(StateStoreBasedWatermarkStorage stateStoreBasedWatermarkStorage) {
    final AtomicBoolean stop = new AtomicBoolean(true);
    if (this.cli.hasOption(WATCH.getOpt())) {
      stop.set(false);
    }

    try {
      if (!stop.get()) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
          public void run() {
            stop.set(true);
          }
        });
      }
      do {
        boolean foundWatermark = false;
        try {
          for (CheckpointableWatermarkState wmState : stateStoreBasedWatermarkStorage.getAllCommittedWatermarks()) {
            foundWatermark = true;
            System.out.println(wmState.getProperties());
          }
        } catch (IOException ie) {
          Throwables.propagate(ie);
        }

        if (!foundWatermark) {
          System.out.println("No watermarks found.");
        }
        if (!stop.get()) {
          Thread.sleep(1000);
        }
      } while (!stop.get());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
