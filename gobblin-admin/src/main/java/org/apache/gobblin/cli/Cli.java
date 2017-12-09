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

package org.apache.gobblin.cli;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.collect.ImmutableMap;

/**
 * A command line interface for interacting with Gobblin.
 * From this tool, you should be able to:
 *  * Check the status of Gobblin jobs
 *  * View ...
 *
 * @author ahollenbach@nerdwallet.com
 */
public class Cli {
  private static final Map<String, Command> commandList =
          ImmutableMap.of(
                  "jobs", (Command)new JobCommand()
          );

  static class GlobalOptions {
    private final String adminServerHost;
    private final int adminServerPort;

    public GlobalOptions(String adminServerHost, int adminServerPort) {
      this.adminServerHost = adminServerHost;
      this.adminServerPort = adminServerPort;
    }

    public String getAdminServerHost() {
      return adminServerHost;
    }

    public int getAdminServerPort() {
      return adminServerPort;
    }
  }

  /**
   * Get the list of valid command names
   * @return List of command names
   */
  public static Collection<String> getCommandNames() {
    return commandList.keySet();
  }

  // Option long codes
  private static final String HOST_OPT = "host";
  private static final String PORT_OPT = "port";

  private static final String DEFAULT_REST_SERVER_HOST = "localhost";
  private static final int DEFAULT_REST_SERVER_PORT = 8080;


  private String[] args;
  private Options options;

  public static void main(String[] args) {
    Cli cli = new Cli(args);
    cli.parseAndExecuteCommand();
  }

  /**
   * Create a new Cli object.
   * @param args Command line arguments
     */
  public Cli(String[] args) {
    this.args = args;

    this.options = new Options();

    this.options.addOption("H", HOST_OPT, true, "Specify host (default:" + DEFAULT_REST_SERVER_HOST + ")");
    this.options.addOption("P", PORT_OPT, true, "Specify port (default:" + DEFAULT_REST_SERVER_PORT + ")");
  }

  /**
   * Parse and execute the appropriate command based on the args.
   * The general flow looks like this:
   *
   * 1. Parse a set of global options (eg host/port for the admin server)
   * 2. Parse out the command name
   * 3. Pass the global options and any left over parameters to a command handler
   */
  public void parseAndExecuteCommand() {
    CommandLineParser parser = new DefaultParser();
    try {
      CommandLine parsedOpts = parser.parse(this.options, this.args, true);
      GlobalOptions globalOptions = createGlobalOptions(parsedOpts);

      // Fetch the command and fail if there is ambiguity
      String[] remainingArgs = parsedOpts.getArgs();
      if (remainingArgs.length == 0) {
        printHelpAndExit("Command not specified!");
      }

      String commandName = remainingArgs[0].toLowerCase();
      remainingArgs = remainingArgs.length > 1 ?
            Arrays.copyOfRange(remainingArgs, 1, remainingArgs.length) :
            new String[]{};

      Command command = commandList.get(commandName);
      if (command == null) {
        System.out.println("Command " + commandName + " not known.");
        printHelpAndExit();
      } else {
        command.execute(globalOptions, remainingArgs);
      }
    } catch (ParseException e) {
      printHelpAndExit("Ran into an error parsing args.");
    }
  }

  /**
   * Build the GlobalOptions information from the raw parsed options
   * @param parsedOpts Options parsed from the cmd line
   * @return
     */
  private GlobalOptions createGlobalOptions(CommandLine parsedOpts) {
    String host = parsedOpts.hasOption(HOST_OPT) ?
            parsedOpts.getOptionValue(HOST_OPT) : DEFAULT_REST_SERVER_HOST;
    int port = DEFAULT_REST_SERVER_PORT;
    try {
      if (parsedOpts.hasOption(PORT_OPT)) {
        port = Integer.parseInt(parsedOpts.getOptionValue(PORT_OPT));
      }
    } catch (NumberFormatException e) {
      printHelpAndExit("The port must be a valid integer.");
    }

    return new GlobalOptions(host, port);
  }

  /**
   * Print help and exit with a success code (0).
   */
  private void printHelpAndExit() {
    System.out.println("Common usages:");
    System.out.println("  gobblin-admin.sh jobs --list");
    System.out.println("  gobblin-admin.sh jobs --list --name JobName");
    System.out.println("  gobblin-admin.sh jobs --details --id job_id");
    System.out.println("  gobblin-admin.sh jobs --properties --<id|name> <job_id|JobName>");
    System.out.println();

    printHelpAndExit(0);
  }

  /**
   * Prints an error message, then prints the help and exits with an error code.
   */
  private void printHelpAndExit(String errorMessage) {
    System.err.println(errorMessage);
    printHelpAndExit(1);
  }

  /**
   * Print help and exit with the specified code.
   * @param exitCode The code to exit with
   */
  private void printHelpAndExit(int exitCode) {
    HelpFormatter hf = new HelpFormatter();

    hf.printHelp("gobblin-admin.sh <command> [options]", this.options);
    System.out.println("Valid commands:");
    for (String command : getCommandNames()) {
      System.out.println(command);
    }

    System.exit(exitCode);
  }
}
