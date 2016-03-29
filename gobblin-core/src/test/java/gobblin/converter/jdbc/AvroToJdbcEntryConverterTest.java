/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.converter.jdbc;

import static org.mockito.Mockito.*;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.converter.SchemaConversionException;
import gobblin.writer.Destination.DestinationType;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"gobblin.converter"})
public class AvroToJdbcEntryConverterTest {

  @Test
  public void testDateConversion() throws IOException, SchemaConversionException, SQLException {
    final String table = "users";
    Map<String, JDBCType> dateColums = new HashMap<>();
    dateColums.put("date_of_birth", JDBCType.DATE);
    dateColums.put("last_modified", JDBCType.TIME);
    dateColums.put("created", JDBCType.TIMESTAMP);

    JdbcWriterCommands mockWriterCommands = mock(JdbcWriterCommands.class);
    when(mockWriterCommands.retrieveDateColumns(mock(Connection.class), table)).thenReturn(dateColums);

    JdbcWriterCommandsFactory factory = mock(JdbcWriterCommandsFactory.class);
    when(factory.newInstance(any(State.class))).thenReturn(mockWriterCommands);

    List<JdbcEntryMetaDatum> jdbcEntryMetaData = new ArrayList<>();
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("name", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("favorite_number", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("favorite_color", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("date_of_birth", JDBCType.DATE));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("last_modified", JDBCType.TIME));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("created", JDBCType.TIMESTAMP));
    JdbcEntrySchema expected = new JdbcEntrySchema(jdbcEntryMetaData);

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/fieldPickInput.avsc"));
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.appendToListProp(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, table);
    AvroToJdbcEntryConverter converter = new AvroToJdbcEntryConverter(workUnitState, factory);
    converter = spy(converter);
    doReturn(null).when(converter).createConnection(workUnitState);
    when(converter.createConnection(workUnitState)).thenReturn(null);
    JdbcEntrySchema actual = converter.convertSchema(inputSchema, workUnitState);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFieldNameConversion() throws IOException, SchemaConversionException, SQLException {
    Map<String, JDBCType> dateColums = new HashMap<>();
    dateColums.put("last_updated", JDBCType.TIMESTAMP);

    final String table = "users";
    JdbcWriterCommands mockWriterCommands = mock(JdbcWriterCommands.class);
    when(mockWriterCommands.retrieveDateColumns(mock(Connection.class), table)).thenReturn(dateColums);

    JdbcWriterCommandsFactory factory = mock(JdbcWriterCommandsFactory.class);
    when(factory.newInstance(any(State.class))).thenReturn(mockWriterCommands);

    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.appendToListProp(ConfigurationKeys.JDBC_PUBLISHER_FINAL_TABLE_NAME, table);
    String fieldPairJson = "{\"userId\":\"user_id\" , \"memberId\":\"member_id\" , \"businessUnit\":\"business_unit\", \"geoRegion\":\"geo_region\", \"superRegion\":\"super_region\", \"subRegion\":\"sub_region\"}";
    workUnitState.appendToListProp(ConfigurationKeys.CONVERTER_AVRO_JDBC_ENTRY_FIELDS_PAIRS, fieldPairJson);
    workUnitState.appendToListProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, DestinationType.MYSQL.name());

    AvroToJdbcEntryConverter converter = new AvroToJdbcEntryConverter(workUnitState, factory);
    converter = spy(converter);
    doReturn(null).when(converter).createConnection(workUnitState);
    when(converter.createConnection(workUnitState)).thenReturn(null);

    Schema inputSchema = new Schema.Parser().parse(getClass().getResourceAsStream("/converter/user.avsc"));

    List<JdbcEntryMetaDatum> jdbcEntryMetaData = new ArrayList<>();
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("user_id", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("member_id", JDBCType.BIGINT));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("business_unit", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("level", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("geo_region", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("super_region", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("sub_region", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("currency", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("segment", JDBCType.VARCHAR));
    jdbcEntryMetaData.add(new JdbcEntryMetaDatum("vertical", JDBCType.VARCHAR));

    JdbcEntrySchema expected = new JdbcEntrySchema(jdbcEntryMetaData);
    JdbcEntrySchema actual = converter.convertSchema(inputSchema, workUnitState);

    Assert.assertEquals(expected, actual);
  }
}