/* (c) 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.jdbc;

import static org.testng.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.mockrunner.mock.jdbc.MockResultSet;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.jdbc.JdbcCommand;
import gobblin.source.extractor.extract.jdbc.JdbcCommandOutput;
import gobblin.source.extractor.extract.jdbc.JdbcExtractor;
import gobblin.source.extractor.extract.jdbc.OracleExtractor;


@Test(groups = { "gobblin.source.extractor.extract.jdbc" })
public class OracleExtractorTest {

	private final static List<MockJdbcColumn> COLUMNS = ImmutableList.of(new MockJdbcColumn("id", "1", Types.INTEGER),
      new MockJdbcColumn("name", "name_1", Types.VARCHAR), new MockJdbcColumn("age", "20", Types.INTEGER));

	private final static String QUERY_1 = "SELECT * FROM x WHERE ROWNUM <= 532";
	private final static String QUERY_2 = "SELECT * FROM x WHERE ROWNUM <= 5 AND x.a < 10";
	private final static String QUERY_3 = "SELECT * FROM x WHERE x.a < 10 AND ROWNUM <= 50";
	private final static String QUERY_EMPTY = "";
	private final static String QUERY_REG = "SELECT * FROM x WHERE x.a < 10";
	
	@Test
	public void testConstructSampleClause() throws Exception {
	    CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
	    output.put(new JdbcCommand(), buildMockResultSet());

	    State state = new WorkUnitState();
	    state.setId("id");
	    OracleExtractor oracleExtractor = new OracleExtractor((WorkUnitState) state);
	    String sClause = oracleExtractor.constructSampleClause();
	    assertEquals(sClause.trim(), (" rownum <= " + oracleExtractor.getSampleRecordCount()).trim());
	}

	@Test
	public void testRemoveSampleClauseFromQuery() throws Exception {
		String q1Clean = "SELECT * FROM x";
		String q2Clean = "SELECT * FROM x WHERE x.a < 10";
		String qEmptyClean = "";

	    CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
	    output.put(new JdbcCommand(), buildMockResultSet());

	    State state = new WorkUnitState();
	    state.setId("id");
	    OracleExtractor oracleExtractor = new OracleExtractor((WorkUnitState) state);

	    String q1Parsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_1);
	    String q2Parsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_2);
	    String q3Parsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_3);
	    String qEmptyParsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_EMPTY);
	    String qRegParsed = oracleExtractor.removeSampleClauseFromQuery(QUERY_REG);

	    assertEquals(q1Clean, q1Parsed);
	    assertEquals(q2Clean, q2Parsed);
	    assertEquals(q2Clean, q3Parsed);
	    assertEquals(qEmptyClean, qEmptyParsed);
	    assertEquals(q2Clean, qRegParsed);		

	}

	@Test
	public void testExractSampleRecordCountFromQuery() throws Exception {
	    CommandOutput<JdbcCommand, ResultSet> output = new JdbcCommandOutput();
	    output.put(new JdbcCommand(), buildMockResultSet());

	    State state = new WorkUnitState();
	    state.setId("id");
	    OracleExtractor oracleExtractor = new OracleExtractor((WorkUnitState) state);	
	    long l1 = oracleExtractor.extractSampleRecordCountFromQuery(QUERY_1);
	    long l2 = oracleExtractor.extractSampleRecordCountFromQuery(QUERY_2);
	    long l3 = oracleExtractor.extractSampleRecordCountFromQuery(QUERY_3);
	    long l4 = oracleExtractor.extractSampleRecordCountFromQuery(QUERY_EMPTY);
	    long l5 = oracleExtractor.extractSampleRecordCountFromQuery(QUERY_REG);

	    assertEquals(l1, (long)532);
	    assertEquals(l2, (long)5);
	    assertEquals(l3, (long)50);
	    assertEquals(l4, (long)-1);
	    assertEquals(l5, (long)-1);	
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
}