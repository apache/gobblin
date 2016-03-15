package gobblin.converter.filter;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.AvroToAvroConverterBase;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.util.AvroUtils;

public class AvroFieldsPickConverter extends AvroToAvroConverterBase {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFieldsPickConverter.class);

  private static final Splitter SPLITTER_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
  private static final Joiner JOINER_ON_DOT = Joiner.on('.');
  private static final Joiner JOINER_ON_COMMA = Joiner.on(',');

  /**
   * Convert the schema to contain only specified field. This will reuse AvroSchemaFieldRemover by listing fields not specified and remove it
   * from schema by using AvroSchemaFieldRemover
   * 1. Retrieve list of fields from property
   * 2. Traverse schema and get list of fields to be removed
   * 3. While traversing also confirm specified fields from property also exist
   * 4. Convert schema by using AvroSchemaFieldRemover
   *
   * {@inheritDoc}
   * @see gobblin.converter.AvroToAvroConverterBase#convertSchema(org.apache.avro.Schema, gobblin.configuration.WorkUnitState)
   */
  @Override
  public Schema convertSchema(Schema inputSchema, WorkUnitState workUnit) throws SchemaConversionException {
    LOG.info("Converting schema " + inputSchema);
    String fieldsStr = workUnit.getProp(ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS);
    Objects.requireNonNull(fieldsStr, ConfigurationKeys.CONVERTER_AVRO_FIELD_PICK_FIELDS + " is required for converter " + this.getClass().getSimpleName());
    LOG.info("Converting schema to selected fields: " + fieldsStr);

    List<String> fields = SPLITTER_ON_COMMA.splitToList(fieldsStr);
    Set<String> fieldsToRemove = fieldsToRemove(inputSchema, fields);
    LOG.info("Fields to be removed from schema: " + fieldsToRemove);
    AvroSchemaFieldRemover remover = new AvroSchemaFieldRemover(JOINER_ON_COMMA.join(fieldsToRemove));
    Schema converted = remover.removeFieldsStrictly(inputSchema);
    LOG.info("Converted schema: " + converted);
    return converted;
  }

  private Set<String> fieldsToRemove(Schema schema, Collection<String> requiredFields) throws SchemaConversionException {
    Set<String> copiedRequiredFields = Sets.newHashSet(requiredFields);
    Set<String> fieldsToRemove = Sets.newHashSet();

    if(!Type.RECORD.equals(schema.getType())) {
      throw new SchemaConversionException("First entry of Avro schema should be a Record type " + schema);
    }
    LinkedList<String> fqn = Lists.<String>newLinkedList();
    for (Field f : schema.getFields()) {
      fieldsToRemoveHelper(f.schema(), f, copiedRequiredFields, fqn, fieldsToRemove);
    }
    if (!copiedRequiredFields.isEmpty()) {
      throw new SchemaConversionException("Failed to pick field(s) as some field(s) " + copiedRequiredFields + " does not exist in Schema " + schema);
    }
    return fieldsToRemove;
  }

  private Set<String> fieldsToRemoveHelper(Schema schema, Field field, Set<String> requiredFields, LinkedList<String> fqn, Set<String> fieldsToRemove) throws SchemaConversionException {
    if(Type.RECORD.equals(schema.getType())) { //Add name of record into fqn and recurse
      fqn.addLast(schema.getFullName());
      for (Field f : schema.getFields()) {
        fieldsToRemoveHelper(f.schema(), f, requiredFields, fqn, fieldsToRemove);
      }
      fqn.removeLast();
      return fieldsToRemove;
    }


    fqn.addLast(Objects.requireNonNull(field).name());
    String fqnStr = JOINER_ON_DOT.join(fqn);
    boolean isRequiredField = requiredFields.remove(fqnStr);

    if(!isRequiredField) {
      boolean isFirstRemoval = fieldsToRemove.add(fqnStr);
      if (!isFirstRemoval) {
        throw new SchemaConversionException("Duplicate name " + fqnStr + " is not allowed");
      }
    }
    fqn.removeLast();
    return fieldsToRemove;
  }

  @Override
  public Iterable<GenericRecord> convertRecord(Schema outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      return new SingleRecordIterable<GenericRecord>(AvroUtils.convertRecordSchema(inputRecord, outputSchema));
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
  }
}
