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

package org.apache.gobblin.converter.jdbc;

import static org.mockito.Mockito.*;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.SchemaConversionException;
import org.apache.gobblin.publisher.JdbcPublisher;
import org.apache.gobblin.writer.Destination.DestinationType;
import org.apache.gobblin.writer.commands.JdbcWriterCommands;
import org.apache.gobblin.writer.commands.JdbcWriterCommandsFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

@Test(groups = {"gobblin.converter"})
public class AvroToJdbcEntryConverterTest {

  @Test
  public void testDateConversion() throws IOException, SchemaConversionException, SQLException {
    final String db = "db";
    final String table = "users";
    Map<String, JdbcType> dateColums = new HashMap<>();
    dateColums.put("date_of_birth", JdbcType.DATE);
    dateColums.put("last_modified", JdbcType.TIME);
    dateColums.put("created", JdbcType.TIMESTAMP);

    JdbcWriterCommands mockWriterCommands = mock(JdbcWriterCommands.class);
    when(mockWriterCommands.retrieveDateColumns(db, table)).thenReturn(dateColums);

    JdbcWriterCommandsFactory factory = mock(JdbcWriterCommandsFactory.class);
    when(factory.newInstance(any(State.class), any(Connection.class))).thenReturn(mockWriterCommands);

    List<JdbcEntryMetaDatum> jdbcEntryMetaData = new ArrayList<>();
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("name", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("favorite_number", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("favorite_color", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("date_of_birth", JdbcType.DATE));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("last_modified", JdbcType.TIME));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("created", JdbcType.TIMESTAMP));
    JdbcEntrySchema expected = new JdbcEntrySchema(jdbcEntryMetaData);

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickInput.avsc"));
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.appendToListProp(JdbcPublisher.JDBC_PUBLISHER_FINAL_TABLE_NAME, table);
    AvroToJdbcEntryConverter converter = new AvroToJdbcEntryConverter(workUnitState);

    Map<String, JdbcType> dateColumnMapping = Maps.newHashMap();
    dateColumnMapping.put("date_of_birth", JdbcType.DATE);
    dateColumnMapping.put("last_modified", JdbcType.TIME);
    dateColumnMapping.put("created", JdbcType.TIMESTAMP);
    workUnitState.appendToListProp(AvroToJdbcEntryConverter.CONVERTER_AVRO_JDBC_DATE_FIELDS,
                                   new Gson().toJson(dateColumnMapping));

    JdbcEntrySchema actual = converter.convertSchema(inputSchema, workUnitState);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFieldNameConversion() throws IOException, SchemaConversionException, SQLException {
    Map<String, JdbcType> dateColums = new HashMap<>();
    dateColums.put("last_updated", JdbcType.TIMESTAMP);

    final String db = "db";
    final String table = "users";
    JdbcWriterCommands mockWriterCommands = mock(JdbcWriterCommands.class);
    when(mockWriterCommands.retrieveDateColumns(db, table)).thenReturn(dateColums);

    JdbcWriterCommandsFactory factory = mock(JdbcWriterCommandsFactory.class);
    when(factory.newInstance(any(State.class), any(Connection.class))).thenReturn(mockWriterCommands);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.appendToListProp(JdbcPublisher.JDBC_PUBLISHER_FINAL_TABLE_NAME, table);
    String fieldPairJson = "{\"userId\":\"user_id\" , \"memberId\":\"member_id\" , \"businessUnit\":\"business_unit\", \"geoRegion\":\"geo_region\", \"superRegion\":\"super_region\", \"subRegion\":\"sub_region\"}";
    workUnitState.appendToListProp(ConfigurationKeys.CONVERTER_AVRO_JDBC_ENTRY_FIELDS_PAIRS, fieldPairJson);
    workUnitState.appendToListProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, DestinationType.MYSQL.name());

    AvroToJdbcEntryConverter converter = new AvroToJdbcEntryConverter(workUnitState);

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/user.avsc"));

    List<JdbcEntryMetaDatum> jdbcEntryMetaData = new ArrayList<>();
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("user_id", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("member_id", JdbcType.BIGINT));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("business_unit", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("level", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("geo_region", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("super_region", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("sub_region", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("currency", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("segment", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("vertical", JdbcType.VARCHAR));

    JdbcEntrySchema expected = new JdbcEntrySchema(jdbcEntryMetaData);

    Map<String, JdbcType> dateColumnMapping = Maps.newHashMap();
    workUnitState.appendToListProp(AvroToJdbcEntryConverter.CONVERTER_AVRO_JDBC_DATE_FIELDS,
                                   new Gson().toJson(dateColumnMapping));
    JdbcEntrySchema actual = converter.convertSchema(inputSchema, workUnitState);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFlattening() throws IOException, SchemaConversionException, SQLException, URISyntaxException, DataConversionException {
    final String db = "db";
    final String table = "users";
    Map<String, JdbcType> dateColums = new HashMap<>();
    dateColums.put("date_of_birth", JdbcType.DATE);
    dateColums.put("last_modified", JdbcType.TIME);
    dateColums.put("created", JdbcType.TIMESTAMP);

    JdbcWriterCommands mockWriterCommands = mock(JdbcWriterCommands.class);
    when(mockWriterCommands.retrieveDateColumns(db, table)).thenReturn(dateColums);

    JdbcWriterCommandsFactory factory = mock(JdbcWriterCommandsFactory.class);
    when(factory.newInstance(any(State.class), any(Connection.class))).thenReturn(mockWriterCommands);

    List<JdbcEntryMetaDatum> jdbcEntryMetaData = new ArrayList<>();
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("name", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("favorite_number", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("favorite_color", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("date_of_birth", JdbcType.DATE));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("last_modified", JdbcType.TIME));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("created", JdbcType.TIMESTAMP));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("nested1_nested1_string", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("nested1_nested1_int", JdbcType.INTEGER));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("nested1_nested2_union_nested2_string", JdbcType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("nested1_nested2_union_nested2_int", JdbcType.INTEGER));
    JdbcEntrySchema expected = new JdbcEntrySchema(jdbcEntryMetaData);

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/pickfields_nested_with_union.avsc"));
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.appendToListProp(JdbcPublisher.JDBC_PUBLISHER_FINAL_TABLE_NAME, table);
    AvroToJdbcEntryConverter converter = new AvroToJdbcEntryConverter(workUnitState);

    Map<String, JdbcType> dateColumnMapping = Maps.newHashMap();
    dateColumnMapping.put("date_of_birth", JdbcType.DATE);
    dateColumnMapping.put("last_modified", JdbcType.TIME);
    dateColumnMapping.put("created", JdbcType.TIMESTAMP);
    workUnitState.appendToListProp(AvroToJdbcEntryConverter.CONVERTER_AVRO_JDBC_DATE_FIELDS,
                                   new Gson().toJson(dateColumnMapping));

    JdbcEntrySchema actualSchema = converter.convertSchema(inputSchema, workUnitState);
    Assert.assertEquals(expected, actualSchema);

    try (
        DataFileReader<GenericRecord> srcDataFileReader =
            new DataFileReader<GenericRecord>(new File(getClass().getResource(
                "/converter/pickfields_nested_with_union.avro").toURI()), new GenericDatumReader<GenericRecord>(
                inputSchema))) {

      List<JdbcEntryData> entries = new ArrayList<>();
      while (srcDataFileReader.hasNext()) {
        JdbcEntryData actualData = converter.convertRecord(actualSchema, srcDataFileReader.next(), workUnitState).iterator().next();
        entries.add(actualData);
      }

      final JsonSerializer<JdbcEntryDatum> datumSer = new JsonSerializer<JdbcEntryDatum>() {
        @Override
        public JsonElement serialize(JdbcEntryDatum datum, Type typeOfSrc, JsonSerializationContext context) {
          JsonObject jso = new JsonObject();
          if (datum.getVal() == null) {
            jso.add(datum.getColumnName(), null);
            return jso;
          }

          if (datum.getVal() instanceof Date) {
            jso.addProperty(datum.getColumnName(), ((Date) datum.getVal()).getTime());
          } else if (datum.getVal() instanceof Timestamp) {
            jso.addProperty(datum.getColumnName(), ((Timestamp) datum.getVal()).getTime());
          } else if (datum.getVal() instanceof Time) {
            jso.addProperty(datum.getColumnName(), ((Time) datum.getVal()).getTime());
          } else {
            jso.addProperty(datum.getColumnName(), datum.getVal().toString());
          }
          return jso;
        }
      };

      JsonSerializer<JdbcEntryData> serializer = new JsonSerializer<JdbcEntryData>() {
        @Override
        public JsonElement serialize(JdbcEntryData src, Type typeOfSrc, JsonSerializationContext context) {
          JsonArray arr = new JsonArray();
          for (JdbcEntryDatum datum : src) {
            arr.add(datumSer.serialize(datum, datum.getClass(), context));
          }
          return arr;
        }
      };

      Gson gson = new GsonBuilder().registerTypeAdapter(JdbcEntryData.class, serializer).serializeNulls().create();

      JsonElement actualSerialized = gson.toJsonTree(entries);
      JsonElement expectedSerialized =
          new JsonParser().parse(new InputStreamReader(getClass().getResourceAsStream("/converter/pickfields_nested_with_union.json")));

      Assert.assertEquals(actualSerialized, expectedSerialized);
    }

    converter.close();
  }
}