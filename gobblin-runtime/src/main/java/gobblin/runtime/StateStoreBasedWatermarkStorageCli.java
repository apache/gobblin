/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.runtime;

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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import lombok.extern.slf4j.Slf4j;

import gobblin.annotation.Alias;
import gobblin.configuration.ConfigurationKeys;
import gobblin.runtime.cli.CliApplication;
import gobblin.runtime.cli.EmbeddedGobblinCliFactory;


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

  @Override
  public void run(String[] args) {
    Options options = new Options();
    options.addOption(HELP);
    options.addOption(ZK);
    options.addOption(JOB_NAME);
    options.addOption(ROOT_DIR);
    options.addOption(WATCH);

    CommandLine cli;
    try {
      CommandLineParser parser = new DefaultParser();
      cli = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
    } catch (ParseException pe) {
      System.out.println( "Command line parse exception: " + pe.getMessage() );
      return;
    }


    if (cli.hasOption(HELP.getOpt())) {
      printUsage(options);
      return;
    }



    TaskState taskState = new TaskState();

    String jobName;
    if (!cli.hasOption(JOB_NAME.getOpt())) {
      log.error("Need Job Name to be specified --", JOB_NAME.getLongOpt());
      throw new RuntimeException("Need Job Name to be specified");
    } else {
      jobName = cli.getOptionValue(JOB_NAME.getOpt());
      log.info("Using job name: {}", jobName);
    }
    taskState.setProp(ConfigurationKeys.JOB_NAME_KEY, jobName);


    String zkAddress = "locahost:2181";
    if (cli.hasOption(ZK.getOpt())) {
      zkAddress = cli.getOptionValue(ZK.getOpt());
    }

    log.info("Using zk address : {}", zkAddress);

    taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_TYPE_KEY, "zk");
    taskState.setProp("state.store.zk.connectString", zkAddress);

    if (cli.hasOption(ROOT_DIR.getOpt())) {
      String rootDir = cli.getOptionValue(ROOT_DIR.getOpt());
      taskState.setProp(StateStoreBasedWatermarkStorage.WATERMARK_STORAGE_CONFIG_PREFIX
          + ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY, rootDir);
      log.info("Setting root dir to {}", rootDir);
    } else {
      log.error("Need root directory specified");
      printUsage(options);
      return;
    }

    StateStoreBasedWatermarkStorage stateStoreBasedWatermarkStorage = new StateStoreBasedWatermarkStorage(taskState);

    final AtomicBoolean stop = new AtomicBoolean(true);

    if (cli.hasOption(WATCH.getOpt())) {
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
      Throwables.propagate(e);
    }
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

}
