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

package gobblin.converter.jdbc;

import static org.mockito.Mockito.*;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.SchemaConversionException;
import gobblin.publisher.JdbcPublisher;
import gobblin.writer.Destination.DestinationType;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;
import com.google.gson.Gson;

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
}