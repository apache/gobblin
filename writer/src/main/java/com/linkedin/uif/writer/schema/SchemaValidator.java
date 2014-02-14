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
}
