package com.linkedin.uif.writer.schema;

/**
 * An enumeration of all supported schema types.
 */
public enum SchemaType {
    /**
     * Indicate a standard Avro schema.
     */
    AVRO,

    /**
     * Indicate a Lumos Avro schema with Lumos-specific metadata.
     */
    LUMOS
}
