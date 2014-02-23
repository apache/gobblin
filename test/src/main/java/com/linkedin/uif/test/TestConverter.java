package com.linkedin.uif.test;

import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.linkedin.uif.converter.ToAvroConverterBase;
import com.linkedin.uif.source.workunit.WorkUnit;

/**
 * An extention to {@link ToAvroConverterBase} for integration test.
 *
 * @author ynli
 */
public class TestConverter extends ToAvroConverterBase<String, String> {

    private static final Gson GSON = new Gson();
    // Expect the input JSON string to be key-value pairs
    private static final Type FIELD_ENTRY_TYPE =
            new TypeToken<Map<String, Object>>(){}.getType();

    @Override
    public Schema convertSchema(String schema, WorkUnit workUnit) {
        return new Schema.Parser().parse(schema);
    }

    @Override
    public GenericRecord convertRecord(Schema schema, String inputRecord,
            WorkUnit workUnit) {

        JsonElement element = GSON.fromJson(inputRecord, JsonElement.class);
        Map<String, Object> fields = GSON.fromJson(element, FIELD_ENTRY_TYPE);
        GenericRecord record = new GenericData.Record(schema);
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            record.put(entry.getKey(), entry.getValue());
        }

        return record;
    }
}
