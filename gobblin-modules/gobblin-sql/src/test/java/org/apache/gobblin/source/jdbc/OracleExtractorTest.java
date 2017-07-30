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

import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.mockrunner.mock.jdbc.MockResultSet;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.CommandOutput;


@Test(groups = { "gobblin.source.jdbc" })
public class OracleExtractorTest {

  private final static List<MockJdbcColumn> COLUMNS = ImmutableList.of(
      new MockJdbcColumn("id", "1", Types.INTEGER),
      new MockJdbcColumn("name", "name_1", Types.VARCHAR),
      new MockJdbcColumn("age", "20", Types.INTEGER));

  private static final String QUERY_1 = "SELECT * FROM x WHERE ROWNUM <= 532";
  private static final String QUERY_2 = "SELECT * FROM x WHERE ROWNUM <= 5 AND x.a < 10";
  private static final String QUERY_3 = "SELECT * FROM x WHERE x.a < 10 AND ROWNUM <= 50";
  private static final String QUERY_4 = "SELECT * FROM x WHERE x.a < 10 AND ROWNUM <= 50 AND x.b = 20";
  private static final String QUERY_EMPTY = "";
  private static final String QUERY_REG = "SELECT * FROM x WHERE x.a < 10";

  private CommandOutput<JdbcCommand, ResultSet> output;
  private State state;
  private OracleExtractor oracleExtractor;

  @BeforeClass
  public void setup() {
    output = new JdbcCommandOutput();
    try {
      output.put(new JdbcCommand(), buildMockResultSet());
    } catch (Exception e) {
      // hack for test failure
      assertEquals("OracleExtractorTest: error initializing mock result set", "false");
    }
    state = new WorkUnitState();
    state.setId("id");
    oracleExtractor = new OracleExtractor((WorkUnitState) state);
  }

  @Test
  public void testConstructSampleClause() throws Exception {
    String sClause = oracleExtractor.constructSampleClause();
    assertEquals(sClause.trim(), (" rownum <= " + oracleExtractor.getSampleRecordCount()).trim());
  }

  @Test
  public void testRemoveSampleClauseFromQuery() throws Exception {
    String q1Expected = "SELECT * FROM x WHERE 1=1";
    String q2Expected = "SELECT * FROM x WHERE 1=1 AND x.a < 10";
    String q3Expected = "SELECT * FROM x WHERE x.a < 10 AND 1=1";
    String q4Expected = "SELECT * FROM x WHERE x.a < 10 AND 1=1 AND x.b = 20";
    String qEmptyClean = "";

    String q1Parsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_1);
    String q2Parsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_2);
    String q3Parsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_3);
    String q4Parsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_4);

    assertEquals(q1Parsed, q1Expected);
    assertEquals(q2Parsed, q2Expected);
    assertEquals(q3Parsed, q3Expected);
    assertEquals(q4Parsed, q4Expected);
  }

  @Test
  public void testExractSampleRecordCountFromQuery() throws Exception {
    long res1 = oracleExtractor.exractSampleRecordCountFromQuery(QUERY_1);
    long res2 = oracleExtractor.exractSampleRecordCountFromQuery(QUERY_2);
    long res3 = oracleExtractor.exractSampleRecordCountFromQuery(QUERY_3);
    long res4 = oracleExtractor.exractSampleRecordCountFromQuery(QUERY_4);
    long res5 = oracleExtractor.exractSampleRecordCountFromQuery(QUERY_EMPTY);
    long res6 = oracleExtractor.exractSampleRecordCountFromQuery(QUERY_REG);

    assertEquals(res1, (long) 532);
    assertEquals(res2, (long) 5);
    assertEquals(res3, (long) 50);
    assertEquals(res4, (long) 50);
    assertEquals(res5, (long) -1);
    assertEquals(res6, (long) -1);
  }

  /**
   * Build a mock implementation of Result using Mockito
   */
  private ResultSet buildMockResultSet() throws Exception {

    MockResultSet mrs = new MockResultSet(StringUtils.EMPTY);
    for (MockJdbcColumn column : COLUMNS) {
      mrs.addColumn(column.getColumnName(), ImmutableList.of(column.getValue()));
    }
    return mrs;
  }
}