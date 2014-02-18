package com.linkedin.uif.writer;

import org.apache.avro.Schema;

import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.writer.converter.SchemaConverter;

/**
 * A test {@link SchemaConverter} that converts a Json-formatted
 * Avro schema string to a {@link Schema}.
 *
 * @author ynli
 */
public class TestSchemaConverter implements SchemaConverter<String, Schema> {

    @Override
    public Schema convert(String sourceSchema) throws SchemaConversionException {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(sourceSchema);
    }
}
