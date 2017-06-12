package gobblin.http;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.SchemaConversionException;

/**
 * A type of {@link HttpJoinConverter} with AVRO as input and output format
 *
 * Input:
 *
 * Output:
 *
 */
@Slf4j
public abstract class AvroHttpJoinConverter<RQ, RP> extends HttpJoinConverter<Schema, Schema, GenericRecord, GenericRecord, RQ, RP> {

  @Override
  public Schema convertSchemaImpl(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {

    Schema httpOutputSchema = new Schema.Parser().parse(workUnit.getProp(CONF_PREFIX + "output.schema"));

    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : inputSchema.getFields()) {
      Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order());
      fields.add(newField);
    }

    Schema.Field newField = new Schema.Field("httpOutput", httpOutputSchema, "http output schema contains request url and return result", null);
    fields.add(newField);

    Schema combinedSchema = Schema.createRecord(inputSchema.getName(), "doc", "gobblin.http", false, fields);
    return combinedSchema;
  }

  /**
   *  Properties "gobblin.converter.http.keys" should be defined in the workUnit
   */
  @Override
  public HttpOperation generateHttpOperation (GenericRecord inputRecord, WorkUnitState workUnit) {
    Map<String, String> keyAndValue = new HashMap<>();
    Iterable<String> keyItrerator = getKeys(workUnit);
    for (String key: keyItrerator) {
      String value = inputRecord.get(key).toString();
      log.info("Http join converter: key is {}, value is {}", key, value);
      keyAndValue.put(key, value);
    }

    HttpOperation operation = new HttpOperation();
    operation.put(0, keyAndValue);

    return operation;
  }

  public Iterable<String> getKeys (WorkUnitState workUnit) {
    String keys = workUnit.getProp(CONF_PREFIX + "keys");
    return Splitter.on(",").omitEmptyStrings().trimResults().split(keys);
  }


  @Override
  public final GenericRecord convertResponse(Schema outputSchema, GenericRecord inputRecord, RQ rawRequest,
      RP response, ResponseStatus status) {

    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    Schema httpOutputSchema = null;
    for (Schema.Field field : outputSchema.getFields()) {
      if (!field.name().equals("httpOutput")) {
        log.info ("copy... " + field.name());
        Object inputValue = inputRecord.get(field.name());
        outputRecord.put(field.name(), inputValue);
      } else {
        httpOutputSchema = field.schema();
      }
    }

    fillHttpOutputData (httpOutputSchema, outputRecord, rawRequest, response, status);
    return outputRecord;
  }

  public abstract void fillHttpOutputData (Schema httpOutputSchema, GenericRecord outputRecord, RQ rawRequest,
      RP response, ResponseStatus status);
}
