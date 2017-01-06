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
package gobblin.cli;

import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.linkedin.r2.RemoteInvocationException;

import gobblin.rest.JobExecutionInfo;
import gobblin.rest.QueryListType;

/**
 * Logic to print out job state
 */
public class JobCommand implements Command {
    private Options options;

    private static class CommandException extends Exception {
      private static final long serialVersionUID = 1L;

        public CommandException(String msg) {
            super(msg);
        }
    }

    private interface SubCommand {
        void execute(CommandLine parsedArgs, AdminClient adminClient, int resultsLimit)
                throws CommandException;
    }

    private static final String HELP_OPT = "help";
    private static final String DETAILS_OPT = "details";
    private static final String LIST_OPT = "list";
    private static final String NAME_OPT = "name";
    private static final String ID_OPT = "id";
    private static final String PROPS_OPT = "properties";

    private static final String RECENT_OPT = "recent";

    private static final int DEFAULT_RESULTS_LIMIT = 10;

    private static final Map<String, SubCommand> subCommandMap =
            ImmutableMap.of(
                    LIST_OPT, new ListAllItemsCommand(),
                    DETAILS_OPT, new ListOneItemWithDetails(),
                    PROPS_OPT, new ListItemsWithPropertiesCommand()
            );


    private SubCommand getAction(CommandLine parsedOpts) {
        for (Map.Entry<String, SubCommand> entry : subCommandMap.entrySet()) {
            if (parsedOpts.hasOption(entry.getKey())) {
                return entry.getValue();
            }
        }

        printHelpAndExit("Unknown subcommand");
        throw new IllegalStateException("unreached...");
    }

    @Override
    public void execute(Cli.GlobalOptions globalOptions, String[] otherArgs) {
        this.options = createCommandLineOptions();
        DefaultParser parser = new DefaultParser();
        AdminClient adminClient = null;

        try {
            CommandLine parsedOpts = parser.parse(options, otherArgs);
            int resultLimit = parseResultsLimit(parsedOpts);
            adminClient = new AdminClient(globalOptions.getAdminServerHost(), globalOptions.getAdminServerPort());
            try {
                getAction(parsedOpts).execute(parsedOpts, adminClient, resultLimit);
            } catch (CommandException e) {
                printHelpAndExit(e.getMessage());
            }
        } catch (ParseException e) {
            printHelpAndExit("Failed to parse jobs arguments: " + e.getMessage());
        } finally {
            if (adminClient != null) adminClient.close();
        }
    }

    private static class ListAllItemsCommand implements SubCommand {
        @Override
        public void execute(CommandLine parsedOpts, AdminClient adminClient, int resultsLimit)
        throws CommandException {
            try {
                if (parsedOpts.hasOption(NAME_OPT)) {
                    JobInfoPrintUtils.printJobRuns(adminClient.queryByJobName(parsedOpts.getOptionValue(NAME_OPT), resultsLimit));
                } else if (parsedOpts.hasOption(RECENT_OPT)) {
                    JobInfoPrintUtils.printAllJobs(adminClient.queryAllJobs(QueryListType.RECENT, resultsLimit), resultsLimit);
                } else {
                    JobInfoPrintUtils.printAllJobs(adminClient.queryAllJobs(QueryListType.DISTINCT, resultsLimit), resultsLimit);
                }
            } catch (RemoteInvocationException e) {
                throw new CommandException("Error talking to adminServer: " + e.getMessage());
            }
        }
    }

    private static class ListOneItemWithDetails implements SubCommand {
        @Override
        public void execute(CommandLine parsedOpts, AdminClient adminClient, int resultsLimit)
                throws CommandException {
            try {
                if (parsedOpts.hasOption(ID_OPT)) {
                    JobInfoPrintUtils.printJob(
                            adminClient.queryByJobId(parsedOpts.getOptionValue(ID_OPT))
                    );
                } else {
                    throw new CommandException("Please specify an id");
                }
            } catch (RemoteInvocationException e) {
                throw new CommandException("Error talking to adminServer: " + e.getMessage());
            }
        }
    }

    private static class ListItemsWithPropertiesCommand implements SubCommand {
        @Override
        public void execute(CommandLine parsedOpts, AdminClient adminClient, int resultsLimit) throws CommandException {
            try {
                if (parsedOpts.hasOption(ID_OPT)) {
                    JobInfoPrintUtils.printJobProperties(
                            adminClient.queryByJobId(parsedOpts.getOptionValue(ID_OPT))
                    );
                } else if (parsedOpts.hasOption(NAME_OPT)) {
                    List<JobExecutionInfo> infos = adminClient.queryByJobName(parsedOpts.getOptionValue(NAME_OPT), 1);
                    if (infos.size() == 0) {
                        System.out.println("No job by that name found");
                    } else {
                        JobInfoPrintUtils.printJobProperties(Optional.of(infos.get(0)));
                    }
                } else {
                    throw new CommandException("Please specify a job id or name");
                }
            } catch (RemoteInvocationException e) {
                throw new CommandException("Error talking to adminServer: " + e.getMessage());
            }
        }
    }

    private Options createCommandLineOptions() {
        Options options = new Options();

        OptionGroup actionGroup = new OptionGroup();
        actionGroup.addOption(new Option("h", HELP_OPT, false, "Shows the help message."));
        actionGroup.addOption(new Option("d", DETAILS_OPT, false, "Show details about a job/task."));
        actionGroup.addOption(new Option("l", LIST_OPT, false, "List jobs/tasks."));
        actionGroup.addOption(new Option("p", PROPS_OPT, false, "Fetch properties with the query."));
        actionGroup.setRequired(true);
        options.addOptionGroup(actionGroup);

        OptionGroup idGroup = new OptionGroup();
        idGroup.addOption(new Option("j", NAME_OPT, true, "Find job(s) matching given job name."));
        idGroup.addOption(new Option("i", ID_OPT, true, "Find the job/task with the given id."));
        options.addOptionGroup(idGroup);

        options.addOption("n", true, "Limit the number of results returned. (default:" + DEFAULT_RESULTS_LIMIT + ")");
        options.addOption("r", RECENT_OPT, false, "List the most recent jobs (instead of a list of unique jobs)");

        return options;
    }

    private int parseResultsLimit(CommandLine parsedOpts) {
        if (parsedOpts.hasOption("n")) {
            try {
                return Integer.parseInt(parsedOpts.getOptionValue("n"));
            } catch (NumberFormatException e) {
                printHelpAndExit("Could not parse integer value for option n.");
                return 0;
            }
        } else {
            return DEFAULT_RESULTS_LIMIT;
        }
    }

    /**
     * Print help and exit with the specified code.
     */
    private void printHelpAndExit(String errorMsg) {
        System.out.println(errorMsg);

        HelpFormatter hf = new HelpFormatter();

        hf.printHelp("gobblin-admin.sh jobs [options]", this.options);

        System.exit(1);
    }
}
