package gobblin.http;

import java.io.IOException;
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
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;

/**
 * A type of {@link HttpJoinConverter} with AVRO as input and output format
 *
 * Input:
 *    User provided record
 *
 * Output:
 *    User provided record plus http request & response record
 */
@Slf4j
public abstract class AvroHttpJoinConverter<RQ, RP> extends HttpJoinConverter<Schema, Schema, GenericRecord, GenericRecord, RQ, RP> {
  public static final String HTTP_REQUEST_RESPONSE = "HttpRequestResponse";

  @Override
  public Schema convertSchemaImpl(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : inputSchema.getFields()) {
      Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order());
      fields.add(newField);
    }

    Schema.Field requestResponseField = new Schema.Field(HTTP_REQUEST_RESPONSE, HttpRequestResponseRecord.getClassSchema(), "http output schema contains request url and return result", null);
    fields.add(requestResponseField);

    Schema combinedSchema = Schema.createRecord(inputSchema.getName(), inputSchema.getDoc() + " (Http request and response are contained)", inputSchema.getNamespace(), false);
    combinedSchema.setFields(fields);
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
      log.debug("Http join converter: key is {}, value is {}", key, value);
      keyAndValue.put(key, value);
    }

    HttpOperation operation = new HttpOperation();
    operation.setKeys(keyAndValue);

    return operation;
  }

  public Iterable<String> getKeys (WorkUnitState workUnit) {
    String keys = workUnit.getProp(CONF_PREFIX + "keys");
    return Splitter.on(",").omitEmptyStrings().trimResults().split(keys);
  }


  @Override
  public final GenericRecord convertResponse(Schema outputSchema, GenericRecord inputRecord, RQ rawRequest,
      RP response) throws DataConversionException {

    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    Schema httpOutputSchema = null;
    for (Schema.Field field : outputSchema.getFields()) {
      if (!field.name().equals(HTTP_REQUEST_RESPONSE)) {
        log.debug ("copy... " + field.name());
        Object inputValue = inputRecord.get(field.name());
        outputRecord.put(field.name(), inputValue);
      } else {
        httpOutputSchema = field.schema();
      }
    }

    try {
      fillHttpOutputData (httpOutputSchema, outputRecord, rawRequest, response);
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
    return outputRecord;
  }

  protected abstract void fillHttpOutputData (Schema httpOutputSchema, GenericRecord outputRecord, RQ rawRequest,
      RP response) throws IOException;
}
