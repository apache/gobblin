package com.linkedin.uif.writer.schema;

/**
 * An enumeration of all supported schema types.
 */
public enum SchemaType {
    /**
     * Indicate a standard Avro schema.
     */
    AVRO(new AvroSchemaValidator()),

    /**
     * Indicate a Lumos Avro schema with Lumos-specific metadata.
     */
    LUMOS(new LumosSchemaValidator());

    // Schema validator associated with the type
    private final SchemaValidator schemaValidator;

    SchemaType(SchemaValidator schemaValidator) {
        this.schemaValidator = schemaValidator;
    }

    /**
     * Get the {@link SchemaValidator} associated with this {@link SchemaType}.
     *
     * @return
     */
    public SchemaValidator getSchemaValidator() {
        return this.schemaValidator;
    }
}
