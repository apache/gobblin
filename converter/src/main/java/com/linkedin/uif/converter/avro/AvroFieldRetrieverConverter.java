package com.linkedin.uif.converter.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.util.AvroUtils;

/**
 * A converter class where the input is an Avro record, and the output is a specific field in that record. Since the
 * field can be of any type this Converter returns a Java {@link Object}. The parameter converter.avro.extractor.field.path
 * specifies the location of the field to retrieve. Nested fields can be specified by following use the following
 * syntax: field.nestedField
 */
public class AvroFieldRetrieverConverter extends Converter<Schema, Object, GenericRecord, Object> {

  private String fieldLocation;

  @Override
  public Converter<Schema, Object, GenericRecord, Object> init(WorkUnitState workUnit) {
    this.fieldLocation = Preconditions.checkNotNull(workUnit.getProp(ConfigurationKeys.CONVERTER_AVRO_EXTRACTOR_FIELD_PATH));
    return this;
  }

  @Override
  public Object convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    return null;
  }

  @Override
  public Object convertRecord(Object outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    Optional<Object> logField = AvroUtils.getField(inputRecord, this.fieldLocation);
    if (logField.isPresent()) {
      return logField.get();
    } else {
      return null;
    }
  }
}