package com.linkedin.uif.converter.avro;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.converter.ToAvroConverterBase;
import com.linkedin.uif.converter.avro.JsonElementConversionFactory.JsonElementConverter;


/**
 * Converts Integra's intermediate data format to avro
 *
 * @author kgoodhop
 *
 */
public class JsonIntermediateToAvroConverter extends ToAvroConverterBase<JsonArray, JsonObject> {
  private Map<String, JsonElementConversionFactory.JsonElementConverter> converters =
      new HashMap<String, JsonElementConversionFactory.JsonElementConverter>();

  @Override
  public Schema convertSchema(JsonArray schema, WorkUnitState workUnit) throws SchemaConversionException {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();

    for (JsonElement elem : schema) {
      JsonObject map = (JsonObject) elem;

      String columnName = map.get("columnName").getAsString();
      String comment = map.get("comment").getAsString();
      Schema fldSchema;

      try {
        JsonElementConverter converter =
            JsonElementConversionFactory.getConvertor(columnName, map.get("dataType").getAsJsonObject().get("type")
                .getAsString(), map, workUnit);
        converters.put(columnName, converter);
        fldSchema = converter.getSchema();
      } catch (UnsupportedDateTypeException e) {
        throw new SchemaConversionException(e);
      }

      fields.add(new Field(columnName, fldSchema, comment, null));
    }

    Schema avroSchema =
        Schema.createRecord(workUnit.getWorkunit().getExtract().getTable(), "", workUnit.getWorkunit().getExtract()
            .getNamespace(), false);
    avroSchema.setFields(fields);

    return avroSchema;
  }

  @Override
  public GenericRecord convertRecord(Schema outputSchema, JsonObject inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    GenericRecord avroRecord = new GenericData.Record(outputSchema);

    for (Map.Entry<String, JsonElement> entry : inputRecord.entrySet()) {
      avroRecord.put(entry.getKey(), converters.get(entry.getKey()).convert(entry.getValue()));

    }

    return avroRecord;
  }

}
