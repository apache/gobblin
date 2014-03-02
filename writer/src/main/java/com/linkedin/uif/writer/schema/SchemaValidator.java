package com.linkedin.uif.writer.schema;

import org.apache.avro.Schema;

/**
 * An interface for classes that validate data schema.
 *
 * @author ynli
 */
public interface SchemaValidator {

    /**
     * Validate the given source schema.
     *
     * @param schema source Avro schema to validate
     * @return whether the source schema is valid
     */
    public boolean validate(Schema schema);
    
    /**
     * Validate the given source schema against the
     * schema from the previous run
     * 
     * @param newSchema is the schema from this run
     * @param oldSchema is the schema from the previous run
     * @return true if schemas are backwards compatible, false otherwise
     */
    public boolean validateAgainstOldSchema(Schema newSchema, Schema oldSchema);
}
