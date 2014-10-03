package com.linkedin.uif.converter.avro;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.linkedin.uif.configuration.ConfigurationKeys;
import com.linkedin.uif.configuration.SourceState;
import com.linkedin.uif.configuration.WorkUnitState;
import com.linkedin.uif.source.workunit.Extract.TableType;


/**
 * Unit test for {@link JsonIntermediateToAvroConverter}
 * @author kgoodhop
 *
 */
@Test(groups = { "com.linkedin.uif.converter" })
public class TestJsonIntermediateToAvroConverter {
  private JsonArray jsonSchema;
  private JsonObject jsonRecord;
  private WorkUnitState state;

  @BeforeClass
  public void setUp() throws Exception {
    Type listType = new TypeToken<JsonArray>() {
    }.getType();
    Gson gson = new Gson();
    jsonSchema =
        gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/schema.json")),
            listType);

    listType = new TypeToken<JsonObject>() {
    }.getType();
    jsonRecord =
        gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream("/record.json")),
            listType);

    SourceState source = new SourceState();
    state =
        new WorkUnitState(source.createWorkUnit(source.createExtract(TableType.SNAPSHOT_ONLY, "test_table",
            "test_namespace")));
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_TIME_FORMAT, "HH:mm:ss");
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "PST");
  }

  @Test
  public void testConverter() throws Exception {
    JsonIntermediateToAvroConverter converter = new JsonIntermediateToAvroConverter();

    Schema avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord record = converter.convertRecord(avroSchema, jsonRecord, state);

    //testing output values are expected types and values
    Assert.assertEquals(jsonRecord.get("Id").getAsString(), record.get("Id").toString());
    Assert.assertEquals(jsonRecord.get("IsDeleted").getAsBoolean(), record.get("IsDeleted"));

    if (!(record.get("Salutation") instanceof GenericArray))
      Assert.fail("expected array, found " + record.get("Salutation").getClass().getName());

    if (!(record.get("MapAccount") instanceof Map))
      Assert.fail("expected map, found " + record.get("MapAccount").getClass().getName());

    Assert.assertEquals(jsonRecord.get("Industry").getAsString(), record.get("Industry").toString());

    DateTimeFormatter format =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

    Assert.assertEquals(jsonRecord.get("LastModifiedDate").getAsString(),
        new DateTime(record.get("LastModifiedDate")).toString(format));
    Assert.assertEquals(jsonRecord.get("date_type").getAsString(),
        new DateTime(record.get("date_type")).toString(format));

//    format = DateTimeFormat.forPattern("HH:mm:ss").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
//    Assert.assertEquals(jsonRecord.get("time_type").getAsString(),
//        new DateTime(record.get("time_type")).toString(format));
    
    Assert.assertEquals(jsonRecord.get("time_type").getAsString(), record.get("time_type").toString());
    Assert.assertEquals(jsonRecord.get("bytes_type").getAsString().getBytes(),
        ((ByteBuffer) record.get("bytes_type")).array());
    Assert.assertEquals(jsonRecord.get("int_type").getAsInt(), record.get("int_type"));
    Assert.assertEquals(jsonRecord.get("long_type").getAsLong(), record.get("long_type"));
    Assert.assertEquals(jsonRecord.get("float_type").getAsFloat(), record.get("float_type"));
    Assert.assertEquals(jsonRecord.get("double_type").getAsDouble(), record.get("double_type"));

    //Testing timezone
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "EST");
    avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord record2 = converter.convertRecord(avroSchema, jsonRecord, state);

    Assert.assertNotEquals(record.get("LastModifiedDate"), record2.get("LastModifiedDate"));
  }

}
