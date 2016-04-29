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

package gobblin.writer.jdbc;

import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.publisher.JdbcPublisher;
import gobblin.source.workunit.WorkUnit;
import gobblin.writer.Destination;
import gobblin.writer.Destination.DestinationType;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;
import gobblin.writer.initializer.JdbcWriterInitializer;

import org.apache.commons.lang.StringUtils;
import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;

import com.google.common.collect.Lists;

@Test(groups = {"gobblin.writer"})
public class JdbcWriterInitializerTest {
  private static final String DEST_TABLE = "dest";
  private static final String STAGING_TABLE = "stage";

  private State state;
  private WorkUnit workUnit;
  private List<WorkUnit> workUnits;
  private JdbcWriterCommandsFactory factory;
  private JdbcWriterCommands commands;
  private JdbcWriterInitializer initializer;
  private Connection conn;

  @BeforeMethod
  private void setup() throws SQLException {
    state = new State();
    state.setProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, DestinationType.MYSQL.name());
    state.setProp(JdbcPublisher.JDBC_PUBLISHER_FINAL_TABLE_NAME, DEST_TABLE);

    workUnit = WorkUnit.createEmpty();
    workUnits = Lists.newArrayList();
    workUnits.add(workUnit);

    factory = mock(JdbcWriterCommandsFactory.class);
    commands = mock(JdbcWriterCommands.class);
    conn = mock(Connection.class);
    doReturn(commands).when(factory).newInstance(any(Destination.class), eq(conn));

    initializer = new JdbcWriterInitializer(state, workUnits, factory, 1, 0);
    initializer = spy(initializer);
    doReturn(conn).when(initializer).createConnection();
  }

  public void skipStagingTable() throws SQLException {
    state.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
    state.setProp(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, Boolean.toString(false));

    initializer.initialize();
    initializer.close();
    Assert.assertEquals(DEST_TABLE, workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));
    verify(commands, never()).createTableStructure( anyString(), anyString());
    verify(commands, never()).truncate(anyString());
    verify(commands, never()).drop(anyString());
  }

  public void skipStagingTableTruncateDestTable() throws SQLException {
    state.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
    state.setProp(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, Boolean.toString(false));
    state.setProp(JdbcPublisher.JDBC_PUBLISHER_REPLACE_FINAL_TABLE, Boolean.toString(true));

    initializer.initialize();
    Assert.assertEquals(DEST_TABLE, workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));

    verify(commands, never()).createTableStructure(anyString(), anyString());
    InOrder inOrder = inOrder(commands);
    inOrder.verify(commands, times(1)).truncate(DEST_TABLE);

    initializer.close();
    inOrder.verify(commands, never()).truncate(anyString());
    verify(commands, never()).drop(anyString());
  }

  public void userCreatedStagingTable() throws SQLException {
    state.setProp(ConfigurationKeys.WRITER_STAGING_TABLE, STAGING_TABLE);
    when(commands.isEmpty(STAGING_TABLE)).thenReturn(Boolean.TRUE);

    initializer.initialize();

    Assert.assertEquals(STAGING_TABLE, workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));
    verify(commands, never()).createTableStructure(anyString(), anyString());
    verify(commands, never()).truncate(anyString());
    verify(commands, never()).drop(anyString());
  }

  public void userCreatedStagingTableTruncate() throws SQLException {
    state.setProp(ConfigurationKeys.WRITER_STAGING_TABLE, STAGING_TABLE);
    state.setProp(ConfigurationKeys.WRITER_TRUNCATE_STAGING_TABLE, Boolean.toString(true));
    when(commands.isEmpty(STAGING_TABLE)).thenReturn(Boolean.TRUE);

    initializer.initialize();
    Assert.assertEquals(STAGING_TABLE, workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));

    InOrder inOrder = inOrder(commands);
    inOrder.verify(commands, times(1)).truncate(STAGING_TABLE);

    initializer.close();
    inOrder.verify(commands, times(1)).truncate(STAGING_TABLE);

    verify(commands, never()).createTableStructure(anyString(), anyString());
    verify(commands, never()).drop(anyString());
  }

  public void initializeWithCreatingStagingTable() throws SQLException {
    when(commands.isEmpty(STAGING_TABLE)).thenReturn(Boolean.TRUE);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(conn.getMetaData()).thenReturn(metadata);
    ResultSet rs = mock(ResultSet.class);
    when(metadata.getTables(anyString(), anyString(), anyString(), any(String[].class))).thenReturn(rs);
    when(rs.next()).thenReturn(Boolean.FALSE);

    initializer.initialize();

    Assert.assertTrue(!StringUtils.isEmpty(workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE)));

    InOrder inOrder = inOrder(commands);
    inOrder.verify(commands, times(1)).createTableStructure(anyString(), anyString());
    inOrder.verify(commands, times(1)).drop( anyString());
    inOrder.verify(commands, times(1)).createTableStructure(anyString(), anyString());

    initializer.close();
    inOrder.verify(commands, times(1)).drop(anyString());
    inOrder.verify(commands, never()).truncate(anyString());
  }
}
