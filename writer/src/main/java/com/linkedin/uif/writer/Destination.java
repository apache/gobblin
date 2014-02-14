package com.linkedin.uif.writer;

import java.util.Properties;

/**
 * A class representing a destination for a writer to write to.
 * It currently supports HDFS and Kafka as destinations.
 *
 * @author ynli
 */
public class Destination {

    /**
     * Enumeration of supported destination types.
     */
    public static enum DestinationType {
        HDFS,
        KAFKA
    }

    // Type of destination
    private final DestinationType type;

    // Destination properties
    private final Properties properties;

    private Destination(DestinationType type, Properties properties) {
        this.type = type;
        this.properties = properties;
    }

    /**
     * Get the destination type.
     *
     * @return destination type
     */
    public DestinationType getType() {
        return this.type;
    }

    /**
     * Get configuration properties for the destination type.
     *
     * @return configuration properties
     */
    public Properties getProperties() {
        return this.properties;
    }

    /**
     * Create a new {@link Destination} instance.
     *
     * @param type destination type
     * @param properties destination properties
     * @return newly created {@link Destination} instance
     */
    public static Destination of(DestinationType type, Properties properties) {
        return new Destination(type, properties);
    }
}
