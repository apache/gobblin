package com.linkedin.uif.writer.schema;

import org.apache.avro.Schema;

/**
 * A {@link SchemaValidator} for standard Avro {@link org.apache.avro.Schema}s.
 *
 * @author ynli
 */
public class AvroSchemaValidator implements SchemaValidator {

    @Override
    public boolean validate(Schema schema) {
        // Always return true
        return true;
    }

    @Override
    public boolean validateAgainstOldSchema(Schema newSchema, Schema oldSchema)
    {
        // TODO How do you check for backwards compatibility?
        return newSchema.equals(oldSchema);
    }
}
