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

import static org.mockito.Mockito.mock;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import gobblin.configuration.State;
import gobblin.converter.jdbc.JdbcEntryData;
import gobblin.converter.jdbc.JdbcEntryDatum;
import gobblin.writer.commands.JdbcBufferedInserter;

@Test(groups = {"gobblin.writer"}, singleThreaded=true)
public abstract class JdbcBufferedInserterTestBase {

  protected Connection conn;
  protected String db = "db";
  protected String table = "stg";

  protected abstract JdbcBufferedInserter getJdbcBufferedInserter(State state, Connection conn);

  @BeforeMethod
  public void setUp() throws Exception {
    this.conn = mock(Connection.class);
  }

  protected List<JdbcEntryData> createJdbcEntries(int colNums, int colSize, int entryCount) {
    Set<String> colNames = new HashSet<>();
    while (colNames.size() < colNums) {
      String colName = RandomStringUtils.randomAlphabetic(colSize);
      if (colNames.contains(colName)) {
        continue;
      }
      colNames.add(colName);
    }

    List<JdbcEntryData> result = new ArrayList<>();
    for (int i = 0; i < entryCount; i++) {
      result.add(createJdbcEntry(colNames, colSize));
    }
    return result;
  }

  private JdbcEntryData createJdbcEntry(Collection<String> colNames, int colSize) {
    List<JdbcEntryDatum> datumList = new ArrayList<>();
    for (String colName : colNames) {
      JdbcEntryDatum datum = new JdbcEntryDatum(colName, RandomStringUtils.randomAlphabetic(colSize));
      datumList.add(datum);
    }
    return new JdbcEntryData(datumList);
  }
}
