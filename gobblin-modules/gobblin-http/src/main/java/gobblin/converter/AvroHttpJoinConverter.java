package gobblin.converter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.http.HttpOperation;
import gobblin.http.HttpRequestResponseRecord;
import gobblin.http.ResponseStatus;
import gobblin.utils.HttpUtils;


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
public abstract class AvroHttpJoinConverter<RQ, RP> extends AsyncHttpJoinConverter<Schema, Schema, GenericRecord, GenericRecord, RQ, RP> {
  public static final String HTTP_REQUEST_RESPONSE_FIELD = "HttpRequestResponse";

  @Override
  public Schema convertSchemaImpl(Schema inputSchema, WorkUnitState workUnitState)
      throws SchemaConversionException {
    List<Schema.Field> fields = Lists.newArrayList();
    for (Schema.Field field : inputSchema.getFields()) {
      Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue(), field.order());
      fields.add(newField);
    }

    Schema.Field requestResponseField = new Schema.Field(HTTP_REQUEST_RESPONSE_FIELD, HttpRequestResponseRecord.getClassSchema(), "http output schema contains request url and return result", null);
    fields.add(requestResponseField);

    Schema combinedSchema = Schema.createRecord(inputSchema.getName(), inputSchema.getDoc() + " (Http request and response are contained)", inputSchema.getNamespace(), false);
    combinedSchema.setFields(fields);
    return combinedSchema;
  }

  /**
   * Extract user defined keys by looking at "gobblin.converter.http.keys"
   * If keys are defined, extract key-value pair from inputRecord and set it to HttpOperation
   * If keys are not defined, generate HttpOperation by HttpUtils.toHttpOperation
   */
  @Override
  protected HttpOperation generateHttpOperation (GenericRecord inputRecord, State state) {
    Map<String, String> keyAndValue = new HashMap<>();
    Optional<Iterable<String>> keys = getKeys(state);
    HttpOperation operation;

    if (keys.isPresent()) {
      for (String key : keys.get()) {
        String value = inputRecord.get(key).toString();
        log.debug("Http join converter: key is {}, value is {}", key, value);
        keyAndValue.put(key, value);
      }
      operation = new HttpOperation();
      operation.setKeys(keyAndValue);
    } else {
      operation = HttpUtils.toHttpOperation(inputRecord);
    }
    return operation;
  }

  private Optional<Iterable<String>> getKeys (State state) {
    if (!state.contains(CONF_PREFIX + "keys")) {
      return Optional.empty();
    }
    Iterable<String> keys = state.getPropAsList(CONF_PREFIX + "keys");
    return Optional.ofNullable(keys);
  }

  @Override
  public final GenericRecord convertRecordImpl(Schema outputSchema, GenericRecord inputRecord, RQ rawRequest, ResponseStatus status) throws DataConversionException {

    GenericRecord outputRecord = new GenericData.Record(outputSchema);
    Schema httpOutputSchema = null;
    for (Schema.Field field : outputSchema.getFields()) {
      if (!field.name().equals(HTTP_REQUEST_RESPONSE_FIELD)) {
        log.debug ("Copy {}", field.name());
        Object inputValue = inputRecord.get(field.name());
        outputRecord.put(field.name(), inputValue);
      } else {
        httpOutputSchema = field.schema();
      }
    }

    try {
      fillHttpOutputData (httpOutputSchema, outputRecord, rawRequest, status);
    } catch (IOException e) {
      throw new DataConversionException(e);
    }
    return outputRecord;
  }

  protected abstract void fillHttpOutputData (Schema httpOutputSchema, GenericRecord outputRecord, RQ rawRequest,
      ResponseStatus status) throws IOException;
}
