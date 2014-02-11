package com.linkedin.uif.scheduler;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.configuration.TaskState;
import com.linkedin.uif.extractor.inputsource.WorkUnit;
import com.linkedin.uif.writer.Destination;
import com.linkedin.uif.converter.DataConverter;
import com.linkedin.uif.converter.SchemaConverter;
import com.linkedin.uif.writer.schema.SchemaType;

import java.util.Properties;

/**
 * A class containing all necessary information to construct
 * and run a {@link Task}.
 */
public class TaskContext<S, D>  {

    private final TaskState taskState;
    private final State contextState;

    /**
     *
     * @param workUnit
     */
    public TaskContext(TaskState taskState) {
        this.taskState = taskState;
    }

    /**
     *
     * @return
     */
    public long getStatusReportingInterval() {
        return 0;
    }

    /**
     * Get the writer {@link Destination.DestinationType}.
     *
     * @return writer {@link Destination.DestinationType}
     */
    public Destination.DestinationType getDestinationType() {
        return Destination.DestinationType.valueOf(
                this.taskState.getProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY,
                                       Destination.DestinationType.HDFS.name()));
    }

    /**
     * Get writer destination {@link Properties}.
     *
     * @return writer destination {@link Properties}
     */
    public Properties getDestinationProperties() {
        Properties properties = new Properties();

        for (String name : this.taskState.getPropertyNames()) {
            // Find properties whose names start with hte pre-defined
            // prefix for all writer destination configuration keys
            if (name.toLowerCase().startsWith(
                    ConfigurationKeys.WRITER_DESTINATION_CONFIG_KEY_PREFIX)) {

                properties.setProperty(name,
                        this.taskState.getProp(name);
            }
        }

        return properties;
    }

    /**
     * Get the {@link DataConverter} used to convert source data records into
     * Avro {@link org.apache.avro.generic.GenericRecord}s.
     *
     * @return the {@link DataConverter}
     */
    @SuppressWarnings("unchecked")
    public DataConverter<D> getDataConverter() {
        try {
            String dataConverterName = this.workUnitProperties.getProperty(
                    ConfigurationKeys.DATA_CONVERTER_CLASS_KEY);
            Object dataConverter = Class.forName(dataConverterName).newInstance();
            if (! (dataConverter instanceof DataConverter)) {
                throw new RuntimeException(String.format(
                        "Class %s is not an implementation of DataConverter",
                        dataConverterName));
            }
            return (DataConverter<D>) dataConverter;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the {@link SchemaConverter} used to convert a source schema into
     * Avro {@link org.apache.avro.Schema}.
     *
     * @return the {@link SchemaConverter}
     */
    @SuppressWarnings("unchecked")
    public SchemaConverter<S> getSchemaConverter() {
        try {
            String schemaConverterName = this.workUnitProperties.getProperty(
                    ConfigurationKeys.SCHEMA_CONVERTER_CLASS_KEY);
            Object schemaConverter = Class.forName(schemaConverterName).newInstance();
            if (! (schemaConverter instanceof SchemaConverter)) {
                throw new RuntimeException(String.format(
                        "Class %s is not an implementation of SchemaConverter",
                        schemaConverterName));
            }
            return (SchemaConverter<S>) schemaConverter;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @return
     */
    public SchemaType getSchemaType() {
        return SchemaType.valueOf(this.workUnitProperties.getProperty(
                ConfigurationKeys.SOURCE_SCHEMA_TYPE_KEY,
                SchemaType.AVRO.name()));
    }
}
