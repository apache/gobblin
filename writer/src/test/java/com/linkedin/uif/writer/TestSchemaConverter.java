package com.linkedin.uif.writer;

import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.converter.SchemaConverter;
import org.apache.avro.Schema;

/**
 * A test {@link SchemaConverter} that converts a Json-formatted
 * Avro schema string to a {@link Schema}.
 */
public class TestSchemaConverter implements SchemaConverter<String> {

    @Override
    public Schema convert(String sourceSchema) throws SchemaConversionException {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(sourceSchema);
    }
}
