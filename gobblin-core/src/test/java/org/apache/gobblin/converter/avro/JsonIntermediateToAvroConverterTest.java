/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.converter.avro;

import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.source.workunit.Extract;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;


/**
 * Unit test for {@link JsonIntermediateToAvroConverter}
 * @author kgoodhop
 *
 */
@Test(groups = {"gobblin.converter"})
public class JsonIntermediateToAvroConverterTest {
  private JsonArray jsonSchema;
  private JsonObject jsonRecord;
  private WorkUnitState state;

  private void initResources(String schemaJson, String recordJson) {
    Type listType = new TypeToken<JsonArray>() {
    }.getType();
    Gson gson = new Gson();
    jsonSchema = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(schemaJson)), listType);

    listType = new TypeToken<JsonObject>() {
    }.getType();
    jsonRecord = gson.fromJson(new InputStreamReader(this.getClass().getResourceAsStream(recordJson)), listType);

    WorkUnit workUnit = new WorkUnit(new SourceState(),
        new Extract(new SourceState(), Extract.TableType.SNAPSHOT_ONLY, "namespace", "dummy_table"));
    state = new WorkUnitState(workUnit);
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_TIME_FORMAT, "HH:mm:ss");
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "PST");
  }

  /**
   * To test schema and record using the path to their resource file.
   * @param schema
   * @param record
   * @param expectedSchema
   * @param expectedRecord
   * @throws SchemaConversionException
   * @throws DataConversionException
   */
  private void complexSchemaTest(String schema, String record, String expectedSchema, String expectedRecord)
      throws SchemaConversionException, DataConversionException {
    initResources(schema, record);
    JsonIntermediateToAvroConverter converter = new JsonIntermediateToAvroConverter();

    Schema avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord genericRecord = converter.convertRecord(avroSchema, jsonRecord, state).iterator().next();

    Assert.assertEquals(avroSchema.toString(), expectedSchema);
    Assert.assertEquals(genericRecord.toString(), expectedRecord);
  }

  @Test
  public void testConverter()
      throws Exception {
    initResources("/converter/schema.json", "/converter/record.json");
    JsonIntermediateToAvroConverter converter = new JsonIntermediateToAvroConverter();

    Schema avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord record = converter.convertRecord(avroSchema, jsonRecord, state).iterator().next();

    //testing output values are expected types and values
    Assert.assertEquals(jsonRecord.get("Id").getAsString(), record.get("Id").toString());
    Assert.assertEquals(jsonRecord.get("IsDeleted").getAsBoolean(), record.get("IsDeleted"));

    if (!(record.get("Salutation") instanceof GenericArray)) {
      Assert.fail("expected array, found " + record.get("Salutation").getClass().getName());
    }

    if (!(record.get("MapAccount") instanceof Map)) {
      Assert.fail("expected map, found " + record.get("MapAccount").getClass().getName());
    }

    Assert.assertEquals(jsonRecord.get("Industry").getAsString(), record.get("Industry").toString());

    DateTimeFormatter format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

    Assert.assertEquals(jsonRecord.get("LastModifiedDate").getAsString(),
        new DateTime(record.get("LastModifiedDate")).toString(format));
    Assert.assertEquals(jsonRecord.get("date_type").getAsString(),
        new DateTime(record.get("date_type")).toString(format));

    format = DateTimeFormat.forPattern("HH:mm:ss").withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
    Assert.assertEquals(jsonRecord.get("time_type").getAsString(),
        new DateTime(record.get("time_type")).toString(format));
    Assert.assertEquals(jsonRecord.get("bytes_type").getAsString().getBytes(),
        ((ByteBuffer) record.get("bytes_type")).array());
    Assert.assertEquals(jsonRecord.get("int_type").getAsInt(), record.get("int_type"));
    Assert.assertEquals(jsonRecord.get("long_type").getAsLong(), record.get("long_type"));
    Assert.assertEquals(jsonRecord.get("float_type").getAsFloat(), record.get("float_type"));
    Assert.assertEquals(jsonRecord.get("double_type").getAsDouble(), record.get("double_type"));

    //Testing timezone
    state.setProp(ConfigurationKeys.CONVERTER_AVRO_DATE_TIMEZONE, "EST");
    avroSchema = converter.convertSchema(jsonSchema, state);
    GenericRecord record2 = converter.convertRecord(avroSchema, jsonRecord, state).iterator().next();

    Assert.assertNotEquals(record.get("LastModifiedDate"), record2.get("LastModifiedDate"));
  }

  @Test
  public void testComplexSchema1()
      throws Exception {
    String expectedSchema =
        "{\"type\":\"record\",\"name\":\"dummy_table\",\"namespace\":\"namespace\",\"doc\":\"\",\"fields\":[{\"name\":\"User\",\"type\":{\"type\":\"record\",\"namespace\":\"\",\"doc\":\"\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\":\"int\",\"source.type\":\"int\"},\"doc\":\"System-assigned numeric user ID. Cannot be changed by the user.\",\"source.type\":\"int\"},{\"name\":\"username\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"The username chosen by the user. Can be changed by the user.\",\"source.type\":\"string\"},{\"name\":\"passwordHash\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"The user's password, hashed using [scrypt](http://www.tarsnap.com/scrypt.html).\",\"source.type\":\"string\"},{\"name\":\"signupDate\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"Timestamp (milliseconds since epoch) when the user signed up\",\"source.type\":\"long\"},{\"name\":\"emailAddresses\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"EmailAddress\",\"doc\":\"\",\"fields\":[{\"name\":\"address\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"The email address, e.g. `foo@example.com`\",\"source.type\":\"string\"},{\"name\":\"verified\",\"type\":{\"type\":\"boolean\",\"source.type\":\"boolean\"},\"doc\":\"true if the user has clicked the link in a confirmation email to this address.\",\"source.type\":\"boolean\"},{\"name\":\"dateAdded\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"Timestamp (milliseconds since epoch) when the email address was added to the account.\",\"source.type\":\"long\"},{\"name\":\"dateBounced\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"Timestamp (milliseconds since epoch) when an email sent to this address last bounced. Reset to null when the address no longer bounces.\",\"source.type\":\"long\"}],\"source.type\":\"record\"},\"source.type\":\"array\"},\"doc\":\"All email addresses on the user's account\",\"source.type\":\"array\"},{\"name\":\"twitterAccounts\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"status\",\"type\":{\"type\":\"enum\",\"name\":\"OAuthStatus\",\"doc\":\"\",\"symbols\":[\"PENDING\",\"ACTIVE\",\"DENIED\",\"EXPIRED\",\"REVOKED\"],\"source.type\":\"enum\"},\"doc\":\"Indicator of whether this authorization is currently active, or has been revoked\",\"source.type\":\"enum\"},{\"name\":\"userId\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"Twitter's numeric ID for this user\",\"source.type\":\"long\"},{\"name\":\"screenName\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"The twitter username for this account (can be changed by the user)\",\"source.type\":\"string\"},{\"name\":\"oauthToken\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"The OAuth token for this Twitter account\",\"source.type\":\"string\"},{\"name\":\"oauthTokenSecret\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"The OAuth secret, used for signing requests on behalf of this Twitter account. `null` whilst the OAuth flow is not yet complete.\",\"source.type\":\"string\"},{\"name\":\"dateAuthorized\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"Timestamp (milliseconds since epoch) when the user last authorized this Twitter account\",\"source.type\":\"long\"}],\"source.type\":\"record\"},\"source.type\":\"array\"},\"doc\":\"All Twitter accounts that the user has OAuthed\",\"source.type\":\"array\"},{\"name\":\"toDoItems\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ToDoItem\",\"doc\":\"\",\"fields\":[{\"name\":\"status\",\"type\":{\"type\":\"enum\",\"name\":\"ToDoStatus\",\"doc\":\"\",\"symbols\":[\"HIDDEN\",\"ACTIONABLE\",\"DONE\",\"ARCHIVED\",\"DELETED\"],\"source.type\":\"enum\"},\"doc\":\"User-selected state for this item (e.g. whether or not it is marked as done)\",\"source.type\":\"enum\"},{\"name\":\"title\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"One-line summary of the item\",\"source.type\":\"string\"},{\"name\":\"description\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"Detailed description (may contain HTML markup)\",\"source.type\":\"string\"},{\"name\":\"snoozeDate\",\"type\":{\"type\":\"long\",\"source.type\":\"long\"},\"doc\":\"Timestamp (milliseconds since epoch) at which the item should go from `HIDDEN` to `ACTIONABLE` status\",\"source.type\":\"long\"}],\"source.type\":\"record\"},\"source.type\":\"array\"},\"doc\":\"The top-level items in the user's to-do list\",\"source.type\":\"array\"}],\"source.type\":\"record\"},\"doc\":\"This is a user record in a fictitious to-do-list management app. It supports arbitrary grouping and nesting of items, and allows you to add items by email or by tweeting.\\n\\nNote this app doesn't actually exist. The schema is just a demo for [Avrodoc](https://github.com/ept/avrodoc)!\",\"source.type\":\"record\"}]}";
    String expectedRecord =
        "{\"User\": {\"signupDate\": 1503651112419, \"emailAddresses\": [{\"dateBounced\": 1503651112419, \"address\": \"vold@example.com\", \"verified\": true, \"dateAdded\": 1503651112419}], \"twitterAccounts\": [{\"oauthTokenSecret\": \"dfsdsds\", \"dateAuthorized\": 1503651112419, \"screenName\": \"hewhomustnotbenamed\", \"oauthToken\": \"sdfsds\", \"userId\": 555, \"status\": \"ACTIVE\"}], \"id\": 1231, \"toDoItems\": [{\"description\": \"AvadaKedavara\", \"title\": \"Kill the boy\", \"snoozeDate\": 1503651112419, \"status\": \"HIDDEN\"}], \"passwordHash\": \"avadakedavara\", \"username\": \"hewhomustnotbenamed\"}}";

    complexSchemaTest("/converter/complex1.json", "/converter/record1.json", expectedSchema, expectedRecord);
  }

  @Test
  public void testComplexSchema2()
      throws Exception {
    String expectedSchema =
        "{\"type\":\"record\",\"name\":\"dummy_table\",\"namespace\":\"namespace\",\"doc\":\"\",\"fields\":[{\"name\":\"protocolv1\",\"type\":{\"type\":\"record\",\"namespace\":\"\",\"doc\":\"\",\"fields\":[{\"name\":\"DependencyType\",\"type\":{\"type\":\"enum\",\"name\":\"depType\",\"doc\":\"\",\"symbols\":[\"REQUIRED\",\"OPTIONAL\"],\"source.type\":\"enum\"},\"doc\":\"\",\"source.type\":\"enum\"},{\"name\":\"DependencyDetail\",\"type\":{\"type\":\"record\",\"doc\":\"\",\"fields\":[{\"name\":\"name\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"A short name of the service who's status is being reported\",\"source.type\":\"string\"},{\"name\":\"descendingField\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"A descending sort order field\",\"source.type\":\"string\"},{\"name\":\"ignoredField\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"An ignored order sort field\",\"source.type\":\"string\"},{\"name\":\"status\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"A string representing the operational status for this service\",\"source.type\":\"string\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"string\",\"source.type\":\"string\"},\"doc\":\"A timestamp showing the time at which this particular dependency was compiled.\",\"source.type\":\"string\"}],\"source.type\":\"record\"},\"doc\":\"Details about status of a dependency.\",\"source.type\":\"record\"}],\"source.type\":\"record\"},\"doc\":\"This is a protocol description for a fictitious network service.\",\"source.type\":\"record\"}]}";
    String expectedRecord =
        "{\"protocolv1\": {\"DependencyDetail\": {\"descendingField\": \"someorder\", \"name\": \"something\", \"ignoredField\": \"somefield\", \"status\": \"somestatus\", \"timestamp\": \"2342323423\"}, \"DependencyType\": \"REQUIRED\"}}";

    complexSchemaTest("/converter/complex2.json", "/converter/record2.json", expectedSchema, expectedRecord);

  }
}
