package com.linkedin.uif.runtime.util;

import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.stream.JsonWriter;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.metastore.FsStateStore;
import com.linkedin.uif.metastore.StateStore;
import com.linkedin.uif.runtime.JobState;

/**
 * A utility class for converting a {@link JobState} object to a json-formatted document.
 *
 * @author ynli
 */
@SuppressWarnings("unused")
public class JobStateToJsonConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobStateToJsonConverter.class);

    private static final String JOB_STATE_STORE_TABLE_SUFFIX = ".jst";

    private final StateStore jobStateStore;
    private final boolean keepConfig;

    public JobStateToJsonConverter(Properties properties, boolean keepConfig) throws IOException {
        this.jobStateStore = new FsStateStore(
                properties.getProperty(
                        ConfigurationKeys.STATE_STORE_FS_URI_KEY,
                        ConfigurationKeys.LOCAL_FS_URI),
                properties.getProperty(ConfigurationKeys.STATE_STORE_ROOT_DIR_KEY),
                JobState.class);
        this.keepConfig = keepConfig;
    }

    /**
     * Convert a single {@link JobState} of the given job instance.
     *
     * @param jobName job name
     * @param jobId job ID
     * @param writer {@link java.io.Writer} to write the json document
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void convert(String jobName, String jobId, Writer writer) throws IOException {
        List<JobState> jobStates = (List<JobState>) this.jobStateStore.getAll(
                jobName, jobId + JOB_STATE_STORE_TABLE_SUFFIX);
        if (jobStates.isEmpty()) {
            LOGGER.warn(String.format("No job state found for job with name %s and id %s",
                    jobName, jobId));
            return;
        }

        JsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.setIndent("\t");
        try {
            // There should be only a single job state
            writeJobState(jsonWriter, jobStates.get(0));
        } finally {
            jsonWriter.close();
        }
    }

    /**
     * Convert the most recent {@link JobState} of the given job.
     *
     * @param jobName job name
     * @param writer {@link java.io.Writer} to write the json document
     */
    @SuppressWarnings("unchecked")
    public void convert(String jobName, Writer writer) throws IOException {
        convert(jobName, "current", writer);
    }

    /**
     * Convert all past {@link JobState}s of the given job.
     *
     * @param jobName job name
     * @param writer {@link java.io.Writer} to write the json document
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public void convertAll(String jobName, Writer writer) throws IOException {
        List<JobState> jobStates = (List<JobState>) this.jobStateStore.getAll(jobName);
        if (jobStates.isEmpty()) {
            LOGGER.warn(String.format("No job state found for job with name %s", jobName));
            return;
        }

        JsonWriter jsonWriter = new JsonWriter(writer);
        jsonWriter.setIndent("\t");
        try {
            writeJobStates(jsonWriter, jobStates);
        } finally {
            jsonWriter.close();
        }
    }

    /**
     * Write a single {@link JobState} to json document.
     *
     * @param jsonWriter {@link com.google.gson.stream.JsonWriter}
     * @param jobState {@link JobState} to write to json document
     * @throws IOException
     */
    private void writeJobState(JsonWriter jsonWriter, JobState jobState) throws IOException {
        jobState.toJson(jsonWriter, this.keepConfig);
    }

    /**
     * Write a list of {@link JobState}s to json document.
     *
     * @param jsonWriter {@link com.google.gson.stream.JsonWriter}
     * @param jobStates list of {@link JobState}s to write to json document
     * @throws IOException
     */
    private void writeJobStates(JsonWriter jsonWriter, List<JobState> jobStates) throws IOException {
        jsonWriter.beginArray();
        for (JobState jobState : jobStates) {
            writeJobState(jsonWriter, jobState);
        }
        jsonWriter.endArray();
    }

    @SuppressWarnings("all")
    public static void main(String[] args) throws Exception {
        Option propertiesOption = OptionBuilder
                .withArgName("gobblin properties file")
                .withDescription("gobblin framework configuration properties file")
                .withLongOpt("properties")
                .hasArgs()
                .isRequired()
                .create('p');
        Option jobNameOption = OptionBuilder
                .withArgName("gobblin job name")
                .withDescription("Gobblin job name")
                .withLongOpt("name")
                .hasArgs()
                .isRequired()
                .create('n');
        Option jobIdOption = OptionBuilder
                .withArgName("gobblin job id")
                .withDescription("Gobblin job id")
                .withLongOpt("id")
                .hasArgs()
                .create('i');
        Option convertAllOption = OptionBuilder
                .withDescription("Whether to convert all past job states of the given job")
                .withLongOpt("all")
                .create('a');
        Option keepConfigOption = OptionBuilder
                .withDescription("Whether to keep all configuration properties")
                .withLongOpt("keepConfig")
                .create("kc");

        Options options = new Options();
        options.addOption(propertiesOption);
        options.addOption(jobNameOption);
        options.addOption(jobIdOption);
        options.addOption(convertAllOption);
        options.addOption(keepConfigOption);

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new BasicParser();
            cmd = parser.parse(options, args);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("JobStateToJsonConverter", options);
            System.exit(1);
        }

        Properties properties = new Properties();
        properties.load(new FileReader(cmd.getOptionValue('p')));
        JobStateToJsonConverter converter = new JobStateToJsonConverter(
                properties, Boolean.valueOf(cmd.getOptionValue("kc")));
        StringWriter stringWriter = new StringWriter();
        if (cmd.hasOption('i')) {
            converter.convert(cmd.getOptionValue('n'), cmd.getOptionValue('i'), stringWriter);
        } else {
            if (cmd.hasOption('a')) {
                converter.convertAll(cmd.getOptionValue('n'), stringWriter);
            } else {
                converter.convert(cmd.getOptionValue('n'), stringWriter);
            }
        }

        System.out.println(stringWriter.toString());
    }
}
