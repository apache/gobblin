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

import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.extract.CommandOutput;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.mockrunner.mock.jdbc.MockResultSet;

import static org.testng.Assert.assertEquals;


@Test(groups = {"gobblin.source.jdbc"})
public class PostgresqlExtractorTest {

  private final static List<MockJdbcColumn> COLUMNS = ImmutableList
      .of(new MockJdbcColumn("id", "1", Types.INTEGER), new MockJdbcColumn("name", "name_1", Types.VARCHAR),
          new MockJdbcColumn("age", "20", Types.INTEGER));

  private static final String QUERY_1 = "SELECT * FROM x WHERE LIMIT 532";
  private static final String QUERY_2 = "SELECT * FROM x WHERE x.a < 10 LIMIT 50";
  private static final String QUERY_3 = "SELECT * FROM x WHERE x.a < 10 AND x.b = 20 LIMIT 50";
  private static final String QUERY_EMPTY = "";
  private static final String QUERY_REG = "SELECT * FROM x WHERE x.a < 10";

  private CommandOutput<JdbcCommand, ResultSet> output;
  private State state;
  private PostgresqlExtractor postgresqlExtractor;

  @BeforeClass
  public void setup() {
    output = new JdbcCommandOutput();
    try {
      output.put(new JdbcCommand(), buildMockResultSet());
    } catch (Exception e) {
      // hack for test failure
      assertEquals("PostgresqlExtractorTest: error initializing mock result set", "false");
    }
    state = new WorkUnitState();
    state.setId("id");
    postgresqlExtractor = new PostgresqlExtractor((WorkUnitState) state);
  }

  @Test
  public void testConstructSampleClause()
      throws Exception {
    String sClause = postgresqlExtractor.constructSampleClause();
    assertEquals(sClause.trim(), (" limit " + postgresqlExtractor.getSampleRecordCount()).trim());
  }

  @Test
  public void testRemoveSampleClauseFromQuery()
      throws Exception {
    String q1Expected = "SELECT * FROM x WHERE 1=1";
    String q2Expected = "SELECT * FROM x WHERE x.a < 10 AND 1=1";
    String q3Expected = "SELECT * FROM x WHERE x.a < 10 AND x.b = 20 AND 1=1";

    String q1Parsed = postgresqlExtractor.removeSampleClauseFromQuery(QUERY_1);
    String q2Parsed = postgresqlExtractor.removeSampleClauseFromQuery(QUERY_2);
    String q3Parsed = postgresqlExtractor.removeSampleClauseFromQuery(QUERY_3);

    assertEquals(q1Parsed, q1Expected);
    assertEquals(q2Parsed, q2Expected);
    assertEquals(q3Parsed, q3Expected);
  }

  @Test
  public void testExractSampleRecordCountFromQuery()
      throws Exception {
    long res1 = postgresqlExtractor.exractSampleRecordCountFromQuery(QUERY_1);
    long res2 = postgresqlExtractor.exractSampleRecordCountFromQuery(QUERY_2);
    long res3 = postgresqlExtractor.exractSampleRecordCountFromQuery(QUERY_3);
    long res4 = postgresqlExtractor.exractSampleRecordCountFromQuery(QUERY_EMPTY);
    long res5 = postgresqlExtractor.exractSampleRecordCountFromQuery(QUERY_REG);

    assertEquals(res1, (long) 532);
    assertEquals(res2, (long) 50);
    assertEquals(res3, (long) 50);
    assertEquals(res4, (long) -1);
    assertEquals(res5, (long) -1);
  }

  @Test
  public void testHourPredicateCondition()
      throws Exception {
    String res1 = postgresqlExtractor.getHourPredicateCondition("my_time", 24L, "h", ">");
    String res2 = postgresqlExtractor.getHourPredicateCondition("my_time", 23L, "HH", ">");
    String res3 = postgresqlExtractor.getHourPredicateCondition("my_time", 2L, "h", ">");

    assertEquals(res1, "my_time > '00'");
    assertEquals(res2, "my_time > '23'");
    assertEquals(res3, "my_time > '02'");
  }

  @Test
  public void testDatePredicateCondition()
      throws Exception {
    String res1 = postgresqlExtractor.getDatePredicateCondition("my_date", 12061992L, "ddMMyyyy", ">");

    assertEquals(res1, "my_date > '1992-06-12'");
  }

  @Test
  public void testTimePredicateCondition()
      throws Exception {
    String res1 = postgresqlExtractor.getTimestampPredicateCondition("my_date", 12061992080809L, "ddMMyyyyhhmmss", ">");

    assertEquals(res1, "my_date > '1992-06-12 08:08:09'");
  }

  /**
   * Build a mock implementation of Result using Mockito
   */
  private ResultSet buildMockResultSet()
      throws Exception {

    MockResultSet mrs = new MockResultSet(StringUtils.EMPTY);
    for (MockJdbcColumn column : COLUMNS) {
      mrs.addColumn(column.getColumnName(), ImmutableList.of(column.getValue()));
    }
    return mrs;
  }
}