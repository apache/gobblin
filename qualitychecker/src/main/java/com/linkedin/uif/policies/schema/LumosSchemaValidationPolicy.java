package com.linkedin.uif.policies.schema;

import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.State;
import com.linkedin.uif.qualitychecker.Policy;

public class LumosSchemaValidationPolicy extends Policy
{

    private static final String ATTRIBUTES_JSON = "attributes_json";

    // Used to parse the attributes_json fields
    private static final Gson GSON = new Gson();
    // Expect the input JSON string to be key-value pairs
    private static final java.lang.reflect.Type FIELD_ENTRY_TYPE =
            new TypeToken<Map<String, Object>>(){}.getType();

    // Top level attributes
    private static final String DUMPDATE = "dumpdate";
    private static final String INSTANCE = "instance";
    private static final String ISFULL = "isFull";
    private static final String TOTAL_RECORDS = "total_records";
    private static final String BEGIN_DATE = "begin_date";
    private static final String END_DATE = "end_date";

    // Set of top level attributes
    private static final ImmutableSet<String> TOP_LEVEL_ATTRIBUTES =
            ImmutableSet.of(
                    DUMPDATE, INSTANCE, ISFULL,
                    TOTAL_RECORDS, BEGIN_DATE, END_DATE);

    // Field level attributes
    private static final String PK = "pk";
    private static final String DELTA = "delta";

    // Set of field level attributes
    private static final ImmutableSet<String> FIELD_LEVEL_ATTRIBUTES =
            ImmutableSet.of(PK, DELTA);
    
    private final Schema schema;
    
    public LumosSchemaValidationPolicy(State state, Type type)
    {
        super(state, type);
        this.schema = new Schema.Parser().parse(state.getProp(ConfigurationKeys.WRITER_OUTPUT_SCHEMA));
    }

    @Override
    public Result executePolicy()
    {
        String attrJson = schema.getProp(ATTRIBUTES_JSON);
        if (Strings.isNullOrEmpty(attrJson)) {
            return Result.FAILED;
        }

        Map<String, Object> attributes = GSON.fromJson(attrJson, FIELD_ENTRY_TYPE);
        if (validateTopLevelAttributes(attributes)) {
            return validateFieldLevelAttributes(schema.getFields());
        }

        return Result.FAILED;
    }

    /**
     * Validate top level attributes.
     */
    private boolean validateTopLevelAttributes(Map<String, Object> attributes) {
        if (!TOP_LEVEL_ATTRIBUTES.equals(attributes.keySet())) {
            return false;
        }

        if (!(attributes.get(DUMPDATE) instanceof String) ||
                Strings.isNullOrEmpty((String) attributes.get(DUMPDATE))) {
            return false;
        }

        if (!(attributes.get(INSTANCE) instanceof String) ||
                Strings.isNullOrEmpty((String) attributes.get(INSTANCE))) {
            return false;
        }

        if (!(attributes.get(ISFULL) instanceof Boolean)) {
            return false;
        }

        if (!(attributes.get(TOTAL_RECORDS) instanceof Long) ||
                !(attributes.get(BEGIN_DATE) instanceof Long) ||
                !(attributes.get(END_DATE) instanceof Long)) {
            return false;
        }

        long beginDate = (Long) attributes.get(BEGIN_DATE);
        long endDate = (Long) attributes.get(END_DATE);
        if (beginDate < 0 || endDate < 0) {
            return false;
        }

        return true;
    }

    /**
     * Validate field level attributes.
     */
    private Result validateFieldLevelAttributes(List<Schema.Field> fields) {
        for (Schema.Field field : fields) {
            String attrJson = field.getProp(ATTRIBUTES_JSON);
            if (Strings.isNullOrEmpty(attrJson)) {
                return Result.FAILED;
            }

            Map<String, Object> attributes = GSON.fromJson(attrJson, FIELD_ENTRY_TYPE);

            if (!attributes.keySet().containsAll(FIELD_LEVEL_ATTRIBUTES)) {
                return Result.FAILED;
            }

            if (!(attributes.get(PK) instanceof Boolean)) {
                return Result.FAILED;
            }

            if (!(attributes.get(DELTA) instanceof Boolean)) {
                return Result.FAILED;
            }
        }
        return Result.PASSED;
    }
}
