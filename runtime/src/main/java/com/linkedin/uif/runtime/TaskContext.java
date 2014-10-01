package com.linkedin.uif.runtime;

import java.util.Collections;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.ForkOperator;
import com.linkedin.uif.publisher.TaskPublisher;
import com.linkedin.uif.publisher.TaskPublisherBuilderFactory;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyChecker;
import com.linkedin.uif.qualitychecker.row.RowLevelPolicyCheckerBuilderFactory;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckResults;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyChecker;
import com.linkedin.uif.qualitychecker.task.TaskLevelPolicyCheckerBuilderFactory;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.WriterOutputFormat;

/**
 * A class containing all necessary information to construct and run a {@link Task}.
 *
 * @author ynli
 */
public class TaskContext {

    private final WorkUnitState workUnitState;
    private final WorkUnit workUnit;

    public TaskContext(WorkUnitState workUnitState) {
        this.workUnitState = workUnitState;
        this.workUnit = workUnitState.getWorkunit();
    }

    /**
     * Get a {@link TaskState}.
     *
     * @return a {@link TaskState}
     */
    public TaskState getTaskState() {
        return new TaskState(this.workUnitState);
    }

    /**
     * Get the {@link Source} used to get the {@link WorkUnit}.
     *
     * @return the {@link Source} used to get the {@link WorkUnit}, <em>null</em>
     *         if it fails to instantiate a {@link Source} object of the given class.
     */
    public Source getSource() {
        try {
            return (Source) Class.forName(
                    this.workUnit.getProp(ConfigurationKeys.SOURCE_CLASS_KEY)).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the interval for status reporting.
     *
     * @return interval for status reporting
     */
    public long getStatusReportingInterval() {
        return this.workUnit.getPropAsLong(
                ConfigurationKeys.TASK_STATUS_REPORT_INTERVAL_IN_MS_KEY,
                ConfigurationKeys.DEFAULT_TASK_STATUS_REPORT_INTERVAL_IN_MS);
    }

    /**
     * Get the writer {@link Destination.DestinationType}.
     *
     * @param branches number of forked branches
     * @param index branch index
     * @return writer {@link Destination.DestinationType}
     */
    public Destination.DestinationType getDestinationType(int branches, int index) {
        return Destination.DestinationType.valueOf(this.workUnit.getProp(
                ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY + (branches > 1 ? "." + index : ""),
                Destination.DestinationType.HDFS.name()));
    }

    /**
     * Get the output format of the writer of type {@link WriterOutputFormat}.
     *
     * @param branches number of forked branches
     * @param index branch index
     * @return output format of the writer
     */
    public WriterOutputFormat getWriterOutputFormat(int branches, int index) {
        return WriterOutputFormat.valueOf(this.workUnit.getProp(
                ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY + (branches > 1 ? "." + index : ""),
                WriterOutputFormat.AVRO.name()));
    }

    /**
     * Get the list of {@link Converter}s to be applied to the source schema
     * and data records before they are handed over to the writer.
     *
     * @return list (possibly empty) of {@link Converter}s
     */
    public List<Converter> getConverters() {
        if (!this.workUnit.contains(ConfigurationKeys.CONVERTER_CLASSES_KEY) ||
                Strings.isNullOrEmpty(this.workUnit.getProp(ConfigurationKeys.CONVERTER_CLASSES_KEY))) {
            return Collections.emptyList();
        }

        // Get the comma-separated list of converter classes
        String converterClassesList = this.workUnit.getProp(ConfigurationKeys.CONVERTER_CLASSES_KEY);
        List<String> converterClasses = Lists.newLinkedList(
                Splitter.on(",").omitEmptyStrings().trimResults().split(converterClassesList));

        List<Converter> converters = Lists.newArrayList();
        for (String converterClass : converterClasses) {
            try {
                Converter converter = (Converter) Class.forName(converterClass).newInstance();
                converter.init(this.workUnitState);
                converters.add(converter);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return converters;
    }

    /**
     * Get the {@link ForkOperator} to be applied to converted input schema and data record.
     *
     * @return {@link ForkOperator} to be used or <code>null</code> if none is specified
     */
    public ForkOperator getForkOperator() {
        try {
            return (ForkOperator) Class.forName(
                    this.workUnit.getProp(
                            ConfigurationKeys.FORK_OPERATOR_CLASS_KEY,
                            ConfigurationKeys.DEFAULT_FORK_OPERATOR_CLASS))
                    .newInstance();
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Get a {@link RowLevelPolicyChecker} for executing row-level
     * {@link com.linkedin.uif.qualitychecker.row.RowLevelPolicy}.
     *
     * @param taskState {@link TaskState} of a {@link Task}
     * @return a {@link RowLevelPolicyChecker}
     */
    public RowLevelPolicyChecker getRowLevelPolicyChecker(TaskState taskState) throws Exception {
        return new RowLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(taskState).build();
    }

    /**
     * Get a {@link TaskLevelPolicyChecker} for executing task-level
     * {@link com.linkedin.uif.qualitychecker.task.TaskLevelPolicy}.
     *
     * @param taskState {@link TaskState} of a {@link Task}
     * @return a {@link TaskLevelPolicyChecker}
     * @throws Exception
     */
    public TaskLevelPolicyChecker getTaskLevelPolicyChecker(TaskState taskState) throws Exception {
        return new TaskLevelPolicyCheckerBuilderFactory().newPolicyCheckerBuilder(taskState).build();
    }

    /**
     * Get a {@link TaskPublisher} for publishing data extracted by a {@link Task}.
     *
     * @param taskState {@link TaskState} of a {@link Task}
     * @param results Task-level policy checking results
     * @return a {@link TaskPublisher}
     */
    public TaskPublisher geTaskPublisher(TaskState taskState, TaskLevelPolicyCheckResults results)
            throws Exception {

        return new TaskPublisherBuilderFactory().newTaskPublisherBuilder(taskState, results).build();
    }
}
