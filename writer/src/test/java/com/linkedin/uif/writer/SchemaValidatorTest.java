package com.linkedin.uif.writer;

import java.io.IOException;

import org.apache.avro.Schema;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.writer.schema.AvroSchemaValidator;
import com.linkedin.uif.writer.schema.SchemaValidator;

public class SchemaValidatorTest
{    
    @Test
    public void testSchemaValidatorSameSchema() throws SchemaConversionException, IOException {
        SchemaValidator schemaValidator = new AvroSchemaValidator();
        Schema schema = new Schema.Parser().parse(TestConstants.AVRO_SCHEMA);
        Assert.assertTrue(schemaValidator.validateAgainstOldSchema(schema, schema));
    }
    
    @Test
    public void testSchemaValidatorDifferentSchema() throws SchemaConversionException, IOException {
        SchemaValidator schemaValidator = new AvroSchemaValidator();
        Schema.Parser parser1 = new Schema.Parser();
        Schema.Parser parser2 = new Schema.Parser();
        Assert.assertFalse(schemaValidator.validateAgainstOldSchema(parser1.parse(TestConstants.AVRO_SCHEMA), parser2.parse(TestConstants.DIFF_AVRO_SCHEMA)));
    }
}
