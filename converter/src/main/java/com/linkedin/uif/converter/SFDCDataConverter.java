package com.linkedin.uif.converter;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Created with IntelliJ IDEA.
 * User: ynli
 * Date: 1/30/14
 * Time: 12:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class SFDCDataConverter implements DataConverter<String> {

    private static final Gson GSON = new Gson();
    // Expect the input JSON string to be key-value pairs
    private static final Type FIELD_ENTRY_TYPE =
            new TypeToken<Map<String, String>>(){}.getType();

    private final Schema schema;

    public SFDCDataConverter(Schema schema) {
        this.schema = schema;
    }

    @Override
    public GenericRecord convert(String json) throws DataConversionException {
        System.out.println(json);
        JsonElement fields = GSON.fromJson(json, JsonObject.class).get("fields");
        Map<String, String> fieldMap = GSON.fromJson(fields, FIELD_ENTRY_TYPE);

        GenericRecord record = new GenericData.Record(this.schema);
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            record.put(entry.getKey(), entry.getValue());
        }

        return record;
    }
}
