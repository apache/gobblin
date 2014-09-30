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
     * @return writer {@link Destination.DestinationType}
     */
    public Destination.DestinationType getDestinationType() {
        return Destination.DestinationType.valueOf(this.workUnit.getProp(
                ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, Destination.DestinationType.HDFS.name()));
    }

    /**
     * Get the output format of the writer of type {@link WriterOutputFormat}.
     *
     * @return output format of the writer
     */
    public WriterOutputFormat getWriterOutputFormat() {
        return WriterOutputFormat.valueOf(this.workUnit.getProp(
                ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY, WriterOutputFormat.AVRO.name()));
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
        if (!this.workUnit.contains(ConfigurationKeys.FORK_OPERATOR_CLASS_KEY)) {
            return null;
        }

        try {
            return (ForkOperator) Class.forName(
                    this.workUnit.getProp(ConfigurationKeys.FORK_OPERATOR_CLASS_KEY)).newInstance();
        } catch (Exception e) {
            return null;
        }
    }
}
