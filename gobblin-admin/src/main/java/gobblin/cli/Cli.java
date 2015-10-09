/* (c) 2015 NerdWallet All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.cli;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.NotImplementedException;

import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

import com.google.common.io.Closer;

import com.linkedin.data.template.StringMap;
import com.linkedin.r2.RemoteInvocationException;

import gobblin.configuration.ConfigurationKeys;
import gobblin.metrics.MetricNames;
import gobblin.rest.JobExecutionInfo;
import gobblin.rest.JobExecutionInfoArray;
import gobblin.rest.JobExecutionInfoClient;
import gobblin.rest.JobExecutionQuery;
import gobblin.rest.JobExecutionQueryResult;
import gobblin.rest.JobStateEnum;
import gobblin.rest.Metric;
import gobblin.rest.MetricArray;
import gobblin.rest.QueryIdTypeEnum;
import gobblin.rest.QueryListType;


/**
 * A command line interface for interacting with Gobblin.
 * From this tool, you should be able to:
 *  * Check the status of Gobblin jobs
 *  * View ...
 *
 * @author ahollenbach@nerdwallet.com
 */
public class Cli {
  private static enum Command {
    JOBS, TASKS
  }

  // Option long codes
  private static final String HOST_OPT = "host";
  private static final String PORT_OPT = "port";
  private static final String HELP_OPT = "help";
  private static final String DETAILS_OPT = "details";
  private static final String LIST_OPT = "list";
  private static final String NAME_OPT = "name";
  private static final String ID_OPT = "id";
  private static final String STATE_OPT = "state";
  private static final String PROPS_OPT = "properties";

  private static final String RECENT_OPT = "recent";

  private static final String DEFAULT_REST_SERVER_HOST = "localhost";
  private static final int DEFAULT_REST_SERVER_PORT = 8080;

  private int resultsLimit = 10;

  private static DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateHourMinuteSecond();
  private static PeriodFormatter periodFormatter = PeriodFormat.getDefault();
  private static NumberFormat decimalFormatter = new DecimalFormat("#0.00");

  private String command;
  private String[] args;
  private Options options;
  private CommandLine parsedOpts;

  private JobExecutionInfoClient client;
  private Closer closer;

  public static void main(String[] args) {
    Cli cli = new Cli(args);
    cli.parse();
    cli.execute();
    cli.close();
  }

  public Cli(String[] args) {
    this.closer = Closer.create();

    this.args = args;

    this.options = new Options();

    this.options.addOption("H", HOST_OPT, true, "Specify host (default:" + DEFAULT_REST_SERVER_HOST + ")");
    this.options.addOption("P", PORT_OPT, true, "Specify port (default:" + DEFAULT_REST_SERVER_PORT + ")");

    OptionGroup actionGroup = new OptionGroup();
    actionGroup.addOption(new Option("h", HELP_OPT, false, "Shows the help message."));
    actionGroup.addOption(new Option("d", DETAILS_OPT, false, "Show details about a job/task."));
    actionGroup.addOption(new Option("l", LIST_OPT, false, "List jobs/tasks."));
    actionGroup.addOption(new Option("p", PROPS_OPT, false, "Fetch properties with the query."));
    actionGroup.setRequired(true);
    this.options.addOptionGroup(actionGroup);

    OptionGroup idGroup = new OptionGroup();
    idGroup.addOption(new Option("j", NAME_OPT, true, "Find job(s) matching given job name."));
    idGroup.addOption(new Option("i", ID_OPT, true, "Find the job/task with the given id."));
    this.options.addOptionGroup(idGroup);

    this.options.addOption("n", true, "Limit the number of results returned. (default:" + resultsLimit + ")");
    this.options.addOption("r", RECENT_OPT, false, "List the most recent jobs (instead of a list of unique jobs)");

  }

  public void parse() {
    CommandLineParser parser = new BasicParser();
    try {
      this.parsedOpts = parser.parse(this.options, this.args);

      if (this.parsedOpts.hasOption(HELP_OPT)) {
        printHelpAndExit();
      }

      // Fetch the command and fail if there is ambiguity
      String[] remainingArgs = this.parsedOpts.getArgs();
      if (remainingArgs.length != 1) {
        printHelpAndExit("Command not specified!");
      }
      this.command = remainingArgs[0];

      parseResultsLimit();
      parseAndConnectToServer();
    } catch (ParseException e) {
      printHelpAndExit("Ran into an error parsing args.");
    }
  }

  private void parseResultsLimit() {
    if (this.parsedOpts.hasOption("n")) {
      try {
        setResultsLimit(Integer.parseInt(this.parsedOpts.getOptionValue("n")));
      } catch (NumberFormatException e) {
        printHelpAndExit("Could not parse integer value for option n.");
      }
    }
  }

  /**
   * Creates a new client with the host and port if specified. Otherwise, uses the defaults localhost and 8080.
   */
  private void parseAndConnectToServer() {
    String host = this.parsedOpts.hasOption(HOST_OPT) ?
        this.parsedOpts.getOptionValue(HOST_OPT) : DEFAULT_REST_SERVER_HOST;
    int port = DEFAULT_REST_SERVER_PORT;
    try {
      if (this.parsedOpts.hasOption(PORT_OPT)) {
        port = Integer.parseInt(this.parsedOpts.getOptionValue(PORT_OPT));
      }
    } catch (NumberFormatException e) {
      printHelpAndExit("The port must be a valid integer.");
    }

    URI serverUri = URI.create(String.format("http://%s:%d/", host, port));
    client = new JobExecutionInfoClient(serverUri.toString());
    closer.register(client);
  }

  public void execute() {
    if (shouldExecuteCommand(Command.JOBS)) {
      executeJobCommand();
    } else if (shouldExecuteCommand(Command.TASKS)) {
      executeTaskCommand();
    } else {
      printHelpAndExit("Command" + this.command + " not found.");
    }

    // End all queries with a newline
    System.out.println();
  }

  private void executeJobCommand() {
    if (this.parsedOpts.hasOption(LIST_OPT)) {
      if (this.parsedOpts.hasOption(NAME_OPT)) {
        printJobTable(queryByJobName(this.parsedOpts.getOptionValue(NAME_OPT)));
      } else if (this.parsedOpts.hasOption(RECENT_OPT)) {
        printAllJobs(queryAllJobs(QueryListType.RECENT));
      } else {
        printAllJobs(queryAllJobs(QueryListType.DISTINCT));
      }
    } else if (this.parsedOpts.hasOption(DETAILS_OPT)) {
      if (this.parsedOpts.hasOption(ID_OPT)) {
        printJob(queryByJobId(this.parsedOpts.getOptionValue(ID_OPT), false));
      } else {
        printHelpAndExit("Please specify an id");
      }
    } else if (this.parsedOpts.hasOption(PROPS_OPT)) {
      if (this.parsedOpts.hasOption(ID_OPT)) {
        printJobProperties(queryByJobId(this.parsedOpts.getOptionValue(ID_OPT), true));
      } else if (this.parsedOpts.hasOption(NAME_OPT)) {
        // Only support property fetching for one job execution
        setResultsLimit(1);
        printJobProperties(queryByJobName(this.parsedOpts.getOptionValue(NAME_OPT)));
      } else {
        printHelpAndExit("Please specify a job id or name");
      }
    }
  }

  private void executeTaskCommand() {
    throw new NotImplementedException("Tasks lookup not yet implemented!");
  }

  private boolean shouldExecuteCommand(Command cmd) {
    return this.command.equals(cmd.name().toLowerCase());
  }

  private JobExecutionInfo queryByJobId(String id, boolean includeProperties) {
    JobExecutionQuery query = new JobExecutionQuery();
    query.setIdType(QueryIdTypeEnum.JOB_ID);
    query.setId(JobExecutionQuery.Id.create(id));
    query.setLimit(1);
    try {
      JobExecutionQueryResult result = this.client.get(query);

      if (result != null &&  result.hasJobExecutions()) {
        return result.getJobExecutions().get(0);
      }
    } catch (RemoteInvocationException e) {
      System.err.println("Error executing query: " + query);
      e.printStackTrace();
      System.exit(1);
    }
    return null;
  }

  private void printJob(JobExecutionInfo jobExecutionInfo) {
    if (jobExecutionInfo == null) {
      System.err.println("Job id not found.");
      System.exit(1);
    }

    List<List<String>> data = new ArrayList<List<String>>();
    List<String> flags = Arrays.asList("", "-");

    data.add(Arrays.asList("Job Name", jobExecutionInfo.getJobName()));
    data.add(Arrays.asList("Job Id", jobExecutionInfo.getJobId()));
    data.add(Arrays.asList("State", jobExecutionInfo.getState().toString()));
    data.add(Arrays.asList("Completed/Launched Tasks",
        String.format("%d/%d", jobExecutionInfo.getCompletedTasks(), jobExecutionInfo.getLaunchedTasks())));
    data.add(Arrays.asList("Start Time", dateTimeFormatter.print(jobExecutionInfo.getStartTime())));
    data.add(Arrays.asList("End Time", dateTimeFormatter.print(jobExecutionInfo.getEndTime())));
    data.add(Arrays.asList("Duration", jobExecutionInfo.getState() == JobStateEnum.COMMITTED ? periodFormatter
        .print(new Period(jobExecutionInfo.getDuration().longValue())) : "-"));
    data.add(Arrays.asList("Tracking URL", jobExecutionInfo.getTrackingUrl()));
    data.add(Arrays.asList("Launcher Type", jobExecutionInfo.getLauncherType().name()));

    new CliTablePrinter.Builder()
        .data(data)
        .flags(flags)
        .delimiterWidth(2)
        .build()
        .printTable();

    printMetrics(jobExecutionInfo.getMetrics());
  }

  private void printMetrics(MetricArray metrics) {
    System.out.println();

    if (metrics.size() == 0) {
      System.out.println("No metrics found.");
      return;
    }

    List<List<String>> data = new ArrayList<List<String>>();
    List<String> flags = Arrays.asList("", "-");

    for (Metric metric : metrics) {
      data.add(Arrays.asList(metric.getName(), metric.getValue()));
    }

    new CliTablePrinter.Builder()
        .data(data)
        .flags(flags)
        .delimiterWidth(2)
        .build()
        .printTable();
  }

  private JobExecutionInfoArray queryByJobName(String name) {
    JobExecutionQuery query = new JobExecutionQuery();
    query.setIdType(QueryIdTypeEnum.JOB_NAME);
    query.setId(JobExecutionQuery.Id.create(name));
    query.setIncludeTaskExecutions(false);
    query.setLimit(this.resultsLimit);
    try {
      JobExecutionQueryResult result = this.client.get(query);

      if (result != null &&  result.hasJobExecutions()) {
        return result.getJobExecutions();
      }
    } catch (RemoteInvocationException e) {
      System.err.println("Error executing query: " + query);
      e.printStackTrace();
      System.exit(1);
    }

    return null;
  }

  private void printJobTable(JobExecutionInfoArray jobExecutionInfos) {
    if (jobExecutionInfos == null) {
      System.err.println("No job executions found.");
      System.exit(1);
    }

    List<String> labels = Arrays.asList("Job Id", "State", "Schedule", "Completed Tasks", "Launched Tasks",
        "Start Time", "End Time", "Duration (s)");
    List<String> flags = Arrays.asList("-", "-", "-", "", "", "-", "-", "-");
    List<List<String>> data = new ArrayList<List<String>>();
    for (JobExecutionInfo jobInfo : jobExecutionInfos) {
      List<String> entry = new ArrayList<String>();
      entry.add(jobInfo.getJobId());
      entry.add(jobInfo.getState().toString());
      entry.add(extractJobSchedule(jobInfo));
      entry.add(jobInfo.getCompletedTasks().toString());
      entry.add(jobInfo.getLaunchedTasks().toString());
      entry.add(dateTimeFormatter.print(jobInfo.getStartTime()));
      entry.add(dateTimeFormatter.print(jobInfo.getEndTime()));
      entry.add(jobInfo.getState() == JobStateEnum.COMMITTED ?
          decimalFormatter.format(jobInfo.getDuration() / 1000.0) : "-");
      data.add(entry);
    }
    new CliTablePrinter.Builder()
        .labels(labels)
        .data(data)
        .flags(flags)
        .delimiterWidth(2)
        .build()
        .printTable();
  }

  private JobExecutionInfo queryJobProperties(String id) {
    JobExecutionQuery query = new JobExecutionQuery();
    query.setIdType(QueryIdTypeEnum.JOB_ID);
    query.setId(JobExecutionQuery.Id.create(id));
    query.setIncludeTaskExecutions(false);
    query.setLimit(1);
    try {
      JobExecutionQueryResult result = this.client.get(query);

      if (result != null &&  result.hasJobExecutions()) {
        return result.getJobExecutions().get(0);
      }
    } catch (RemoteInvocationException e) {
      System.err.println("Error executing query: " + query);
      e.printStackTrace();
      System.exit(1);
    }
    return null;
  }

  private void printJobProperties(JobExecutionInfo jobExecutionInfo) {
    if (jobExecutionInfo == null) {
      System.err.println("Job not found.");
      System.exit(1);
    }

    List<List<String>> data = new ArrayList<List<String>>();
    List<String> flags = Arrays.asList("", "-");
    List<String> labels = Arrays.asList("Property Key", "Property Value");

    for (Map.Entry<String, String> entry : jobExecutionInfo.getJobProperties().entrySet()) {
      data.add(Arrays.asList(entry.getKey(), entry.getValue()));
    }

    new CliTablePrinter.Builder()
        .labels(labels)
        .data(data)
        .flags(flags)
        .delimiterWidth(2)
        .build()
        .printTable();
  }

  private void printJobProperties(JobExecutionInfoArray jobExecutionInfos) {
    if (jobExecutionInfos == null) {
      System.err.println("Job not found.");
      System.exit(1);
    } else {
      printJobProperties(jobExecutionInfos.get(0));
    }
  }

  private JobExecutionInfoArray queryAllJobs(QueryListType lookupType) {
    JobExecutionQuery query = new JobExecutionQuery();
    query.setIdType(QueryIdTypeEnum.LIST_TYPE);
    query.setId(JobExecutionQuery.Id.create(lookupType));

    // Disable properties and task executions (prevents response size from ballooning)
    query.setJobProperties(ConfigurationKeys.JOB_RUN_ONCE_KEY + "," + ConfigurationKeys.JOB_SCHEDULE_KEY);
    query.setIncludeTaskExecutions(false);

    query.setLimit(this.resultsLimit);

    try {
      JobExecutionQueryResult result = this.client.get(query);

      if (result != null && result.hasJobExecutions()) {
        return result.getJobExecutions();
      }
    } catch (RemoteInvocationException e) {
      System.err.println("Error executing query: " + query);
      e.printStackTrace();
      System.exit(1);
    }
    return null;
  }

  private void printAllJobs(JobExecutionInfoArray jobExecutionInfos) {
    if (jobExecutionInfos == null) {
      System.err.println("No jobs found.");
      System.exit(1);
    }

    List<String> labels = Arrays.asList("Job Name", "State", "Last Run Started", "Last Run Completed",
        "Schedule", "Last Run Records Processed", "Last Run Records Failed");
    List<String> flags = Arrays.asList("-", "-", "-", "-", "-", "", "");
    List<List<String>> data = new ArrayList<List<String>>();
    for (JobExecutionInfo jobInfo : jobExecutionInfos) {
      List<String> entry = new ArrayList<String>();
      entry.add(jobInfo.getJobName());
      entry.add(jobInfo.getState().toString());
      entry.add(dateTimeFormatter.print(jobInfo.getStartTime()));
      entry.add(dateTimeFormatter.print(jobInfo.getEndTime()));

      entry.add(extractJobSchedule(jobInfo));

      // Add metrics
      MetricArray metrics = jobInfo.getMetrics();
      Double recordsProcessed = null;
      Double recordsFailed = null;
      try {
        for (Metric metric : metrics) {
          if (metric.getName().equals(MetricNames.ExtractorMetrics.RECORDS_READ_METER)) {
            recordsProcessed = Double.parseDouble(metric.getValue());
          } else if (metric.getName().equals(MetricNames.ExtractorMetrics.RECORDS_FAILED_METER)) {
            recordsFailed = Double.parseDouble(metric.getValue());
          }
        }

        if (recordsProcessed != null && recordsFailed != null) {
          entry.add(recordsProcessed.toString());
          entry.add(recordsFailed.toString());
        }
      } catch(NumberFormatException ex) {
        System.err.println("Failed to process metrics");
      }
      if (recordsProcessed == null || recordsFailed == null) {
        entry.add("-");
        entry.add("-");
      }

      data.add(entry);
    }
    new CliTablePrinter.Builder()
        .labels(labels)
        .data(data)
        .flags(flags)
        .delimiterWidth(2)
        .build()
        .printTable();

    if (jobExecutionInfos.size() == this.resultsLimit) {
      System.out.println("\nWARNING: There may be more jobs (# of results is equal to the limit)");
    }
  }

  /**
   * Extracts the schedule from a job execution.
   *
   * If the job was in run once mode, it will return that, otherwise it will return the schedule.
   * @param jobInfo A job execution info to extract from
   * @return "RUN_ONCE", the Quartz schedule string, or "UNKNOWN" if there were no job properties
   */
  private String extractJobSchedule(JobExecutionInfo jobInfo) {
    if (jobInfo.hasJobProperties() && jobInfo.getJobProperties().size() > 0) {
      StringMap props = jobInfo.getJobProperties();

      if (props.containsKey(ConfigurationKeys.JOB_RUN_ONCE_KEY) ||
          !props.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        return "RUN_ONCE";
      } else if (props.containsKey(ConfigurationKeys.JOB_SCHEDULE_KEY)) {
        return props.get(ConfigurationKeys.JOB_SCHEDULE_KEY);
      }
    }
    return "UNKNOWN";
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

    hf.printHelp("gobblin-admin.sh <jobs|tasks> [options]", this.options);

    close();
    System.exit(exitCode);
  }

  private void setResultsLimit(int limit) {
    this.resultsLimit = limit;
  }

  public void close() {
    try {
      closer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
