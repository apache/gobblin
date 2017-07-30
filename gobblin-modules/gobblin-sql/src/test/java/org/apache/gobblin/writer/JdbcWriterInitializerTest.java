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

package gobblin.writer;

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


@Test(groups = { "gobblin.writer" })
public class JdbcWriterInitializerTest {
  private static final String DB = "db";
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
    this.state = new State();
    this.state.setProp(ConfigurationKeys.WRITER_DESTINATION_TYPE_KEY, DestinationType.MYSQL.name());
    this.state.setProp(JdbcPublisher.JDBC_PUBLISHER_DATABASE_NAME, DB);
    this.state.setProp(JdbcPublisher.JDBC_PUBLISHER_FINAL_TABLE_NAME, DEST_TABLE);

    this.workUnit = WorkUnit.createEmpty();
    this.workUnits = Lists.newArrayList();
    this.workUnits.add(this.workUnit);

    this.factory = mock(JdbcWriterCommandsFactory.class);
    this.commands = mock(JdbcWriterCommands.class);
    this.conn = mock(Connection.class);
    doReturn(this.commands).when(this.factory).newInstance(any(Destination.class), eq(this.conn));

    this.initializer = new JdbcWriterInitializer(this.state, this.workUnits, this.factory, 1, 0);
    this.initializer = spy(this.initializer);
    doReturn(this.conn).when(this.initializer).createConnection();
  }

  public void skipStagingTable() throws SQLException {
    this.state.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
    this.state.setProp(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, Boolean.toString(false));

    this.initializer.initialize();
    this.initializer.close();
    Assert.assertEquals(DEST_TABLE, this.workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));
    verify(this.commands, never()).createTableStructure(anyString(), anyString(), anyString());
    verify(this.commands, never()).truncate(anyString(), anyString());
    verify(this.commands, never()).drop(anyString(), anyString());
  }

  public void skipStagingTableTruncateDestTable() throws SQLException {
    this.state.setProp(ConfigurationKeys.JOB_COMMIT_POLICY_KEY, "partial");
    this.state.setProp(ConfigurationKeys.PUBLISH_DATA_AT_JOB_LEVEL, Boolean.toString(false));
    this.state.setProp(JdbcPublisher.JDBC_PUBLISHER_REPLACE_FINAL_TABLE, Boolean.toString(true));

    this.initializer.initialize();
    Assert.assertEquals(DEST_TABLE, this.workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));

    verify(this.commands, never()).createTableStructure(anyString(), anyString(), anyString());
    InOrder inOrder = inOrder(this.commands);
    inOrder.verify(this.commands, times(1)).truncate(DB, DEST_TABLE);

    this.initializer.close();
    inOrder.verify(this.commands, never()).truncate(anyString(), anyString());
    verify(this.commands, never()).drop(anyString(), anyString());
  }

  public void userCreatedStagingTable() throws SQLException {
    this.state.setProp(ConfigurationKeys.WRITER_STAGING_TABLE, STAGING_TABLE);
    when(this.commands.isEmpty(DB, STAGING_TABLE)).thenReturn(Boolean.TRUE);

    this.initializer.initialize();

    Assert.assertEquals(STAGING_TABLE, this.workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));
    verify(this.commands, never()).createTableStructure(anyString(), anyString(), anyString());
    verify(this.commands, never()).truncate(anyString(), anyString());
    verify(this.commands, never()).drop(anyString(), anyString());
  }

  public void userCreatedStagingTableTruncate() throws SQLException {
    this.state.setProp(ConfigurationKeys.WRITER_STAGING_TABLE, STAGING_TABLE);
    this.state.setProp(ConfigurationKeys.WRITER_TRUNCATE_STAGING_TABLE, Boolean.toString(true));
    when(this.commands.isEmpty(DB, STAGING_TABLE)).thenReturn(Boolean.TRUE);

    this.initializer.initialize();
    Assert.assertEquals(STAGING_TABLE, this.workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE));

    InOrder inOrder = inOrder(this.commands);
    inOrder.verify(this.commands, times(1)).truncate(DB, STAGING_TABLE);

    this.initializer.close();
    inOrder.verify(this.commands, times(1)).truncate(DB, STAGING_TABLE);

    verify(this.commands, never()).createTableStructure(anyString(), anyString(), anyString());
    verify(this.commands, never()).drop(anyString(), anyString());
  }

  public void initializeWithCreatingStagingTable() throws SQLException {
    when(this.commands.isEmpty(DB, STAGING_TABLE)).thenReturn(Boolean.TRUE);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(this.conn.getMetaData()).thenReturn(metadata);
    ResultSet rs = mock(ResultSet.class);
    when(metadata.getTables(anyString(), anyString(), anyString(), any(String[].class))).thenReturn(rs);
    when(rs.next()).thenReturn(Boolean.FALSE);

    this.initializer.initialize();

    Assert.assertTrue(!StringUtils.isEmpty(this.workUnit.getProp(ConfigurationKeys.WRITER_STAGING_TABLE)));

    InOrder inOrder = inOrder(this.commands);
    inOrder.verify(this.commands, times(1)).createTableStructure(anyString(), anyString(), anyString());
    inOrder.verify(this.commands, times(1)).drop(anyString(), anyString());
    inOrder.verify(this.commands, times(1)).createTableStructure(anyString(), anyString(), anyString());

    this.initializer.close();
    inOrder.verify(this.commands, times(1)).drop(anyString(), anyString());
    inOrder.verify(this.commands, never()).truncate(anyString(), anyString());
  }
}
