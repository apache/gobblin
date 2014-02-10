package com.linkedin.uif.writer;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.DataConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * A test {@link DataConverter} that converts a Json-formatted Avro
 * record string into a {@link GenericRecord}.
 */
public class TestDataConverter implements DataConverter<String> {

    private static final Gson GSON = new Gson();
    // Expect the input JSON string to be key-value pairs
    private static final Type FIELD_ENTRY_TYPE =
            new TypeToken<Map<String, Object>>(){}.getType();

    private final Schema schema;

    public TestDataConverter(Schema schema) {
        this.schema = schema;
    }

    @Override
    public GenericRecord convert(String sourceRecord) throws DataConversionException {
        JsonElement element = GSON
                .fromJson(sourceRecord, JsonObject.class)
                .get("fields");
        Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
        GenericRecord record = new GenericData.Record(this.schema);
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            record.put(entry.getKey(), entry.getValue());
        }

        return record;
    }
}
