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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.JdbcPublisher;
import gobblin.writer.commands.JdbcWriterCommands;
import gobblin.writer.commands.JdbcWriterCommandsFactory;

import org.mockito.InOrder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {"gobblin.writer"})
public class JdbcPublisherTest {
  private String database = "db";
  private String stagingTable = "stg";
  private String destinationTable = "dest";
  private State state;
  private JdbcWriterCommands commands;
  private JdbcWriterCommandsFactory factory;
  private Connection conn;
  private WorkUnitState workUnitState;
  private Collection<WorkUnitState> workUnitStates;
  private JdbcPublisher publisher;

  @BeforeMethod
  private void setup() {
    state = new State();
    state.setProp(JdbcPublisher.JDBC_PUBLISHER_FINAL_TABLE_NAME, destinationTable);
    state.setProp(JdbcPublisher.JDBC_PUBLISHER_DATABASE_NAME, database);

    commands = mock(JdbcWriterCommands.class);
    factory = mock(JdbcWriterCommandsFactory.class);
    conn = mock(Connection.class);
    when(factory.newInstance(state, conn)).thenReturn(commands);

    workUnitStates = new ArrayList<>();
    workUnitState = mock(WorkUnitState.class);
    when(workUnitState.getProp(ConfigurationKeys.WRITER_STAGING_TABLE)).thenReturn(stagingTable);
    workUnitStates.add(workUnitState);

    publisher = new JdbcPublisher(state, factory);
    publisher = spy(publisher);
    doReturn(conn).when(publisher).createConnection();
  }

  @AfterMethod
  private void cleanup() throws IOException {
    publisher.close();
  }

  public void testPublish() throws IOException, SQLException {
    publisher.publish(workUnitStates);

    InOrder inOrder = inOrder(conn, commands, workUnitState);

    inOrder.verify(conn, times(1)).setAutoCommit(false);
    inOrder.verify(commands, times(1)).copyTable(database, stagingTable, destinationTable);
    inOrder.verify(workUnitState, times(1)).setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    inOrder.verify(conn, times(1)).commit();
    inOrder.verify(conn, times(1)).close();

    verify(commands, never()).deleteAll(database, destinationTable);
  }

  public void testPublishReplaceOutput() throws IOException, SQLException {
    state.setProp(JdbcPublisher.JDBC_PUBLISHER_REPLACE_FINAL_TABLE, Boolean.toString(true));
    publisher.publish(workUnitStates);

    InOrder inOrder = inOrder(conn, commands, workUnitState);

    inOrder.verify(conn, times(1)).setAutoCommit(false);
    inOrder.verify(commands, times(1)).deleteAll(database, destinationTable);
    inOrder.verify(commands, times(1)).copyTable(database, stagingTable, destinationTable);
    inOrder.verify(workUnitState, times(1)).setWorkingState(WorkUnitState.WorkingState.COMMITTED);
    inOrder.verify(conn, times(1)).commit();
    inOrder.verify(conn, times(1)).close();
  }

  public void testPublishFailure() throws SQLException, IOException {
    doThrow(RuntimeException.class).when(commands).copyTable(database, stagingTable, destinationTable);

    try {
      publisher.publish(workUnitStates);
      Assert.fail("Test case didn't throw Exception.");
    } catch (RuntimeException e) {
      Assert.assertTrue(e instanceof RuntimeException);
    }

    InOrder inOrder = inOrder(conn, commands, workUnitState);

    inOrder.verify(conn, times(1)).setAutoCommit(false);
    inOrder.verify(commands, times(1)).copyTable(database, stagingTable, destinationTable);

    inOrder.verify(conn, times(1)).rollback();
    inOrder.verify(conn, times(1)).close();

    verify(conn, never()).commit();
    verify(commands, never()).deleteAll(database, destinationTable);
    verify(workUnitState, never()).setWorkingState(any(WorkUnitState.WorkingState.class));
  }
}