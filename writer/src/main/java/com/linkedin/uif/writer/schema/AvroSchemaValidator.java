package com.linkedin.uif.writer.schema;

import org.apache.avro.Schema;

/**
 * A {@link SchemaValidator} for standard Avro {@link org.apache.avro.Schema}s.
 */
public class AvroSchemaValidator implements SchemaValidator {

    @Override
    public boolean validate(Schema schema) {
        // Always return true
        return true;
    }
}
