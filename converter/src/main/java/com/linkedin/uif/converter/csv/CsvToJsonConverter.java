package com.linkedin.uif.converter.csv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.converter.Converter;
import com.linkedin.uif.converter.DataConversionException;
import com.linkedin.uif.converter.SchemaConversionException;
import com.linkedin.uif.converter.avro.JsonIntermediateToAvroConverter;
import com.linkedin.uif.source.workunit.WorkUnit;

public class CsvToJsonConverter implements Converter<String, JsonArray, String, JsonObject>
{
    /**
     * Take in an input schema of type string, the schema must be in JSON format
     * @return a JsonArray representation of the schema
     */
    @Override
    public JsonArray convertSchema(String inputSchema, WorkUnitState workUnit) throws SchemaConversionException
    {        
        JsonParser jsonParser = new JsonParser();
        JsonElement jsonSchema = jsonParser.parse(inputSchema);
        return jsonSchema.getAsJsonArray();
    }

    /**
     * Takes in a record with format String and splits the data based on SOURCE_SCHEMA_DELIMITER
     * Uses the inputSchema and the split record to convert the record to a JsonObject
     * @return a JsonObject representing the record
     */
    @Override
    public JsonObject convertRecord(JsonArray outputSchema, String inputRecord, WorkUnitState workUnit) throws DataConversionException
    {
        List<String> recordSplit = Lists.newArrayList(Splitter.onPattern(workUnit.getProp(ConfigurationKeys.CONVERTER_CSV_TO_JSON_DELIMITER)).trimResults().split(inputRecord));
        JsonObject outputRecord = new JsonObject();
        
        for (int i = 0; i < outputSchema.size(); i++) {
            if (i < recordSplit.size()) {
                if (recordSplit.get(i).isEmpty()) {
                    outputRecord.add(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), JsonNull.INSTANCE);
                } else {
                    outputRecord.addProperty(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), recordSplit.get(i));
                }
            } else {
//                Arrays.toString(recordSplit.toArray());
                // TODO something is wrong with this line
//                 outputRecord.add(outputSchema.get(i).getAsString(), JsonNull.INSTANCE);
                outputRecord.add(outputSchema.get(i).getAsJsonObject().get("columnName").getAsString(), JsonNull.INSTANCE);
            }
        }
        return outputRecord;
    }
    
    public static void main(String args[]) {
        String schema = "[{\"columnName\":\"id\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"long\"}},{\"columnName\":\"linkedin_uid\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"snippet\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"published_timestamp\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"long\"}},{\"columnName\":\"image\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"title\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"relevance_score\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"float\"}},{\"columnName\":\"publisher\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}},{\"columnName\":\"url\",\"comment\":\"\",\"isNullable\":\"true\",\"dataType\":{\"type\":\"string\"}}]";
//        String ex = "169872366\t46033622\the\nllo\"world\t1406073604\thttps://s3.amazonaws.com/newsle_article_images/9dc77eee187948b589598604e6e4658b.jpeg\thelloworld\t0.2625\tnj.com\thttp://www.nj.com/south-jersey-voices/index.ssf/2014/07/post_191.html";
//        ex.toString();
        CsvToJsonConverter converter = new CsvToJsonConverter();
        WorkUnitState workUnit = new WorkUnitState();
        workUnit.getWorkunit().getExtract().setProp(ConfigurationKeys.EXTRACT_NAMESPACE_NAME_KEY, "helloworld");
        workUnit.getWorkunit().getExtract().setProp(ConfigurationKeys.EXTRACT_TABLE_NAME_KEY, "helloworld");
        workUnit.setProp(ConfigurationKeys.CONVERTER_CSV_TO_JSON_DELIMITER, "\t");
//        System.out.println(Arrays.toString(Lists.newArrayList(Splitter.onPattern(workUnit.getProp(ConfigurationKeys.CONVERTER_CSV_TO_JSON_DELIMITER)).trimResults().split(ex)).toArray()));;
        try {
            JsonArray arr = converter.convertSchema(schema, null);
            JsonIntermediateToAvroConverter jsonConvert = new JsonIntermediateToAvroConverter();
            Schema schemaAvro = jsonConvert.convertSchema(arr, workUnit);
            BufferedReader br = null;
     
                String sCurrentLine;
     
                br = new BufferedReader(new FileReader("test.1406095244"));
     
                while ((sCurrentLine = br.readLine()) != null) {
                    JsonObject obj = converter.convertRecord(arr, sCurrentLine, workUnit);
                    System.out.println(obj);
                    GenericRecord record = jsonConvert.convertRecord(schemaAvro, obj, workUnit);
                    System.out.println(record);
                }
       } catch (Exception e) {
           e.printStackTrace();
       }
    }
}

