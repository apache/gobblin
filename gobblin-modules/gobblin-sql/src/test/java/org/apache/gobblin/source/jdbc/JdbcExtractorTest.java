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

package gobblin.source.jdbc;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mockrunner.mock.jdbc.MockResultSet;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.exception.SchemaException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;


@Test(groups = { "gobblin.source.jdbc" })
public class JdbcExtractorTest {

  private final static List<MockJdbcColumn> COLUMNS = ImmutableList.of(new MockJdbcColumn("id", "1", Types.INTEGER),
      new MockJdbcColumn("name", "name_1", Types.VARCHAR), new MockJdbcColumn("age", "20", Types.INTEGER));

  @Test
  public void testGetData() throws Exception {

    CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
    output.put(new JdbcCommand(), buildMockResultSet());

    State state = new WorkUnitState();
    state.setId("id");
    JdbcExtractor jdbcExtractor = new MysqlExtractor((WorkUnitState) state);

    List<String> columnNames = Lists.newArrayListWithCapacity(COLUMNS.size());

    for (MockJdbcColumn mockJdbcColumn:COLUMNS) {
      columnNames.add(mockJdbcColumn.getColumnName());
    }

    jdbcExtractor.setHeaderRecord(columnNames);

    Iterator<JsonElement> itr = jdbcExtractor.getData(output);

    // Make sure there is an element in the iterator
    assertTrue(itr.hasNext());

    JsonObject obj = itr.next().getAsJsonObject();

    // Verify the columns
    for (MockJdbcColumn column : COLUMNS) {
      assertEquals(obj.get(column.getColumnName()).getAsString(), column.getValue());
    }
  }

  /*
   * Build a mock implementation of Result using Mockito
   */
  private ResultSet buildMockResultSet() throws Exception {

    MockResultSet mrs = new MockResultSet(StringUtils.EMPTY);

    for (MockJdbcColumn column : COLUMNS) {
      mrs.addColumn(column.getColumnName(), ImmutableList.of(column.getValue()));
    }

    return mrs;
  }

  /**
   * Test for the metadata query to see if the check for unsigned int is present
   */
  @Test
  public void testUnsignedInt() throws SchemaException {
    State state = new WorkUnitState();
    state.setId("id");
    MysqlExtractor mysqlExtractor = new MysqlExtractor((WorkUnitState) state);

    List<Command> commands = mysqlExtractor.getSchemaMetadata("db", "table");

    assertTrue(commands.get(0).getCommandType() == JdbcCommand.JdbcCommandType.QUERY);
    assertTrue(commands.get(0).getParams().get(0).contains("bigint"));
    assertTrue(commands.get(1).getCommandType() == JdbcCommand.JdbcCommandType.QUERYPARAMS);
    assertTrue(!commands.get(1).getParams().get(0).contains("unsigned"));

    // set option to promote unsigned int to bigint
    state.setProp(ConfigurationKeys.SOURCE_QUERYBASED_PROMOTE_UNSIGNED_INT_TO_BIGINT, "true");
    commands = mysqlExtractor.getSchemaMetadata("db", "table");

    assertTrue(commands.get(0).getCommandType() == JdbcCommand.JdbcCommandType.QUERY);
    assertTrue(commands.get(0).getParams().get(0).contains("bigint"));
    assertTrue(commands.get(1).getCommandType() == JdbcCommand.JdbcCommandType.QUERYPARAMS);
    assertTrue(commands.get(1).getParams().get(0).contains("unsigned"));
  }

  public void testHasJoinOperation() {
    boolean result;
    // no space
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a,b");
    Assert.assertTrue(result);

    // has space
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a aliasA , b aliasB");
    Assert.assertTrue(result);
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a , b");
    Assert.assertTrue(result);
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a ,     b");
    Assert.assertTrue(result);

    // simple query
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a");
    Assert.assertFalse(result);
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a where a.id=\"hello,world\"");
    Assert.assertFalse(result);

    // complex query
    result = JdbcExtractor.hasJoinOperation(
        "select a.fromLoc from (Select dest as fromLoc, id from b) as a, c where a.id < c.id");
    Assert.assertTrue(result);
  }
}
