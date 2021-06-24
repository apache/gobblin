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

package org.apache.gobblin.source.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mockrunner.mock.jdbc.MockResultSet;
import com.mockrunner.mock.jdbc.MockResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.exception.SchemaException;
import org.apache.gobblin.source.extractor.extract.Command;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


@Test(groups = { "gobblin.source.jdbc" })
public class JdbcExtractorTest {

  private static final List<MockJdbcColumn> COLUMNS = ImmutableList.of(new MockJdbcColumn("id", "1", Types.INTEGER),
      new MockJdbcColumn("name", "name_1", Types.VARCHAR), new MockJdbcColumn("age", "20", Types.INTEGER));

  private static final String TIME_COLUMN = "time";

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
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a , b  limit 100");
    Assert.assertTrue(result);
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a limit 100");
    Assert.assertFalse(result);
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a ,     b");
    Assert.assertTrue(result);

    // simple query
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a");
    Assert.assertFalse(result);
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a where a.id=\"hello,world\"");
    Assert.assertFalse(result);
    result = JdbcExtractor.hasJoinOperation("select a.fromLoc from a where a.id=\"hello,world\" limit 100");
    Assert.assertFalse(result);

    // complex query
    result = JdbcExtractor.hasJoinOperation(
        "select a.fromLoc from (Select dest as fromLoc, id from b) as a, c where a.id < c.id");
    Assert.assertTrue(result);
    result = JdbcExtractor.hasJoinOperation(
        "select a.fromLoc from (Select dest as fromLoc, id from b) as a, c where a.id < c.id limit 10");
    Assert.assertTrue(result);
    result = JdbcExtractor.hasJoinOperation(
        "select a.fromLoc from (Select dest as fromLoc, id from b) as a limit 10");
    Assert.assertFalse(result);
  }

  /**
   * Helper function to build MockTimestampResultSet containing a single timestamp column.
   * @param testCases the list of test cases
   * @param behavior the expected behavior for the MockTimestampResultSet
   * @return a MockTimestampResultSet containing the test cases
   */
  private ResultSet buildMockResultSetForTimeColumn(List<String> testCases, String behavior) {
    MockResultSetMetaData mrsMetaData = new MockResultSetMetaData();
    mrsMetaData.setColumnCount(1);
    mrsMetaData.setColumnName(1, TIME_COLUMN);
    mrsMetaData.setColumnType(1, Types.TIMESTAMP);

    MockTimestampResultSet mrs = new MockTimestampResultSet(StringUtils.EMPTY, behavior);
    mrs.setResultSetMetaData(mrsMetaData);
    mrs.addColumn(TIME_COLUMN, testCases);

    return mrs;
  }

  /**
   * Helper function to test when zeroDateTimeBehavior is set.
   * @param testCases A LinkedHashMap mapping the input timestamp as a string to the expected output.
   *                  We use LinkedHashMap to preserve the order of the inputs/outputs.
   * @param zeroDateTimeBehavior the expected behavior of a zero timestamp. Should be one of these values:
   *                             null, "CONVERT_TO_NULL", "ROUND", "EXCEPTION"
   * @throws Exception
   */
  private void testZeroDateTimeBehavior(LinkedHashMap<String, String> testCases, String zeroDateTimeBehavior) throws Exception {
    WorkUnitState workUnitState = new WorkUnitState();
    workUnitState.setId("id");
    if (zeroDateTimeBehavior != null) {
      workUnitState.setProp(ConfigurationKeys.SOURCE_CONN_PROPERTIES, "zeroDateTimeBehavior=" + zeroDateTimeBehavior);
    }

    JdbcExtractor jdbcExtractor = new MysqlExtractor(workUnitState);
    jdbcExtractor.setHeaderRecord(Collections.singletonList(TIME_COLUMN));

    CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
    output.put(new JdbcCommand(), buildMockResultSetForTimeColumn(new ArrayList<>(testCases.keySet()), zeroDateTimeBehavior));


    Iterator<JsonElement> dataIterator = jdbcExtractor.getData(output);

    // Make sure there is an element in the iterator
    assertTrue(dataIterator.hasNext());

    // Iterate through the output and verify that they are equal to the expected output
    Iterator<String> expectedIterator = testCases.values().iterator();
    while (dataIterator.hasNext()) {
      JsonElement element = dataIterator.next().getAsJsonObject().get(TIME_COLUMN);
      String expectedString = expectedIterator.next();

      if (element.isJsonNull()) {
        assert expectedString == null;
      } else {
        assertEquals(element.getAsString(), expectedString);
      }
    }
  }

  // zeroDateTimeBehavior=CONVERT_TO_NULL
  // Zero timestamps should be converted to null.
  // Other timestamps should be returned formatted as "yyyy-MM-dd HH:mm:ss".
  public void testZeroDateTimeBehaviorConvertToNull() throws Exception {
    LinkedHashMap<String, String> testCases = new LinkedHashMap<>();
    testCases.put("2000-01-01 12:34:56.789", "2000-01-01 12:34:56");
    testCases.put("1999-12-12 13:14:15.16", "1999-12-12 13:14:15");
    testCases.put("0000-00-00 00:00:00.0", null);

    testZeroDateTimeBehavior(testCases, "CONVERT_TO_NULL");
  }

  // zeroDateTimeBehavior=ROUND
  // Zero timestamps should be converted to "0001-01-01 00:00:00".
  // Other timestamps should be returned formatted as "yyyy-MM-dd HH:mm:ss".
  public void testZeroDateTimeBehaviorRound() throws Exception {
    LinkedHashMap<String, String> testCases = new LinkedHashMap<>();
    testCases.put("2000-01-01 12:34:56.789", "2000-01-01 12:34:56");
    testCases.put("1999-12-12 13:14:15.16", "1999-12-12 13:14:15");
    testCases.put("0000-00-00 00:00:00.0", "0001-01-01 00:00:00");

    testZeroDateTimeBehavior(testCases, "ROUND");
  }

  // zeroDateTimeBehavior=EXCEPTION
  // Zero timestamps should cause an exception to be thrown.
  // Other timestamps should be returned formatted as "yyyy-MM-dd HH:mm:ss".
  public void testZeroDateTimeBehaviorException() throws Exception {
    LinkedHashMap<String, String> testThrows = new LinkedHashMap<>();
    testThrows.put("0000-00-00 00:00:00", "this value is irrelevant");

    Assert.assertThrows(() -> testZeroDateTimeBehavior(testThrows, "EXCEPTION"));

    LinkedHashMap<String, String> testPasses = new LinkedHashMap<>();
    testPasses.put("2000-01-01 12:34:56.789", "2000-01-01 12:34:56");

    testZeroDateTimeBehavior(testPasses, "EXCEPTION");
  }

  // zeroDateTimeBehavior is not set.
  // All timestamps should be returned formatted as "yyyy-MM-dd HH:mm:ss"
  public void testZeroDateTimeBehaviorNotSpecified() throws Exception {
    LinkedHashMap<String, String> testCases = new LinkedHashMap<>();
    testCases.put("2000-01-01 12:34:56.789", "2000-01-01 12:34:56");
    testCases.put("1999-12-12 13:14:15.16", "1999-12-12 13:14:15");
    testCases.put("0000-00-00 00:00:00.0", "0000-00-00 00:00:00");

    testZeroDateTimeBehavior(testCases, null);
  }
}
