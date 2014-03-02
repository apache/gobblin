package com.linkedin.uif.scheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.*;
import com.linkedin.uif.source.Source;
import com.linkedin.uif.source.workunit.WorkUnit;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.writer.WriterOutputFormat;
import com.linkedin.uif.writer.converter.DataConverter;
import com.linkedin.uif.writer.converter.SchemaConverter;
import com.linkedin.uif.writer.schema.SchemaType;

/**
 * A class containing all necessary information to construct
 * and run a {@link Task}.
 *
 * @author ynli
 */
public class TaskContext {

    private final WorkUnitState workUnitState;
    private final WorkUnit workUnit;

    // This is the converter that converts the source schema and data ecords
    // into the target schema and data records expected by the writer
    private Converter converterForWriter;

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
                    this.workUnit.getProp(ConfigurationKeys.SOURCE_CLASS_KEY))
                    .newInstance();
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
        return Destination.DestinationType.valueOf(
                this.workUnit.getProp(
                        ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY,
                        Destination.DestinationType.HDFS.name()));
    }

    /**
     * Get writer destination {@link Properties}.
     *
     * @return writer destination {@link Properties}
     */
    public Properties getDestinationProperties() {
        Properties properties = new Properties();

        for (String name : this.workUnit.getPropertyNames()) {
            // Find properties whose names start with hte pre-defined
            // prefix for all writer destination configuration keys
            if (name.toLowerCase().startsWith(
                    ConfigurationKeys.WRITER_PREFIX)) {

                properties.setProperty(name, this.workUnit.getProp(name));
            }
        }

        return properties;
    }

    /**
     * Get the output format of the writer of type {@link WriterOutputFormat}.
     *
     * @return output format of the writer
     */
    public WriterOutputFormat getWriterOutputFormat() {
        return WriterOutputFormat.valueOf(this.workUnit.getProp(
                ConfigurationKeys.WRITER_OUTPUT_FORMAT_KEY,
                WriterOutputFormat.AVRO.name()));
    }

    /**
     * Get the {@link DataConverter} used to convert source data records into
     * Avro {@link org.apache.avro.generic.GenericRecord}s.
     *
     * @param schemaForWriter data schema ready for the writer
     *
     * @return the {@link DataConverter}
     */
    @SuppressWarnings("unchecked")
    public DataConverter getDataConverter(final Object schemaForWriter) {
        final Converter converter = getConverterForWriter();
        return new DataConverter() {

            @Override
            public Object convert(Object sourceRecord) throws DataConversionException {
                return converter.convertRecord(converter.convertSchema(
                        schemaForWriter, workUnitState), sourceRecord, workUnitState);
            }
        };
    }

    /**
     * Get the {@link SchemaConverter} used to convert a source schema into
     * Avro {@link org.apache.avro.Schema}.
     *
     * @return the {@link SchemaConverter}
     */
    @SuppressWarnings("unchecked")
    public SchemaConverter getSchemaConverter() {
        final Converter converter = getConverterForWriter();
        return new SchemaConverter() {

            @Override
            public Object convert(Object sourceSchema) throws SchemaConversionException {
                return converter.convertSchema(sourceSchema, workUnitState);
            }
        };
    }

    /**
     * Get the type of the source schema as a
     * {@link com.linkedin.uif.writer.schema.SchemaType}.
     *
     * @return type of the source schema
     */
    public SchemaType getSchemaType() {
        return SchemaType.valueOf(this.workUnit.getProp(
                ConfigurationKeys.SOURCE_SCHEMA_TYPE_KEY,
                SchemaType.AVRO.name()));
    }

    /**
     * Get the {@link Converter} that converts the source schema and data
     * records into the target schema and data records expected by the writer.
     */
    private Converter getConverterForWriter() {
        if (this.converterForWriter == null) {
            // Get the comma-separated list of converter classes
            String converterClassesList = this.workUnit.getProp(
                    ConfigurationKeys.CONVERTER_CLASSES_KEY);
            // Get the last converter class which is assumed to be for the writer
            String converterClassForWriter = Lists.newLinkedList(
                    Splitter.on(",")
                            .omitEmptyStrings()
                            .trimResults()
                            .split(converterClassesList))
                    .getLast();
            try {
                this.converterForWriter = (Converter) Class.forName(
                        converterClassForWriter).newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return this.converterForWriter;
    }

    /**
     * Get the list of {@link Converter}s to be applied to the source schema
     * and data records before they are handed over to the writer.
     *
     * @return list of {@link Converter}s
     */
    public List<Converter> getConverters() {
        // Get the comma-separated list of converter classes
        String converterClassesList = this.workUnit.getProp(
                ConfigurationKeys.CONVERTER_CLASSES_KEY);
        LinkedList<String> converterClasses = Lists.newLinkedList(
                Splitter.on(",")
                        .omitEmptyStrings()
                        .trimResults()
                        .split(converterClassesList));
        // Remove the last one which is assumed to be for the writer
        converterClasses.removeLast();

        List<Converter> converters = Lists.newArrayList();
        for (String converterClass : converterClasses) {
            try {
                Converter converter =
                        (Converter) Class.forName(converterClass).newInstance();
                converters.add(converter);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return converters;
    }
}
