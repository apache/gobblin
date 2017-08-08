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
package org.apache.gobblin.data.management.retention.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * A Proof of concept to represent Retention policies as SQL queries. The POC uses Apache Derby in-memory database to
 * store directory structure metadata.
 */
public class SqlBasedRetentionPoc {

  private static final int TWO_YEARS_IN_DAYS = 365 * 2;
  private static final String DAILY_PARTITION_PATTERN = "yyyy/MM/dd";

  private BasicDataSource basicDataSource;
  private Connection connection;

  /**
   * <ul>
   * <li>Create the in-memory database and connect
   * <li>Create tables for snapshots and daily_paritions
   * <li>Attach all user defined functions from {@link SqlUdfs}
   * </ul>
   *
   */
  @BeforeClass
  public void setup() throws SQLException {
    basicDataSource = new BasicDataSource();
    basicDataSource.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
    basicDataSource.setUrl("jdbc:derby:memory:derbypoc;create=true");

    Connection connection = basicDataSource.getConnection();
    connection.setAutoCommit(false);
    this.connection = connection;

    execute("CREATE TABLE Snapshots (dataset_path VARCHAR(255), name VARCHAR(255), path VARCHAR(255), ts TIMESTAMP, row_count bigint)");
    execute("CREATE TABLE Daily_Partitions (dataset_path VARCHAR(255), path VARCHAR(255), ts TIMESTAMP)");

    // Register UDFs
    execute("CREATE FUNCTION TIMESTAMP_DIFF(timestamp1 TIMESTAMP, timestamp2 TIMESTAMP, unitString VARCHAR(50)) RETURNS BIGINT PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'org.apache.gobblin.data.management.retention.sql.SqlUdfs.timestamp_diff'");
  }

  @AfterClass
  public void cleanUp() throws Exception {
    basicDataSource.close();
  }

  /**
   *
   * The test inserts a few test snapshots. A query is issued to retrieve the two most recent snapshots
   */
  @Test
  public void testKeepLast2Snapshots() throws Exception {

    insertSnapshot(new Path("/data/databases/Forum/Comments/1453743903767-PT-440505235"));
    insertSnapshot(new Path("/data/databases/Forum/Comments/1453830569999-PT-440746131"));
    insertSnapshot(new Path("/data/databases/Forum/Comments/1453860526464-PT-440847244"));
    insertSnapshot(new Path("/data/databases/Forum/Comments/1453889323804-PT-440936752"));

    // Derby does not support LIMIT keyword. The suggested workaround is to setMaxRows in the PreparedStatement
    PreparedStatement statement = connection.prepareStatement("SELECT name FROM Snapshots ORDER BY ts desc");
    statement.setMaxRows(2);

    ResultSet rs = statement.executeQuery();

    // Snapshots to be retained
    rs.next();
    Assert.assertEquals(rs.getString(1), "1453889323804-PT-440936752");
    rs.next();
    Assert.assertEquals(rs.getString(1), "1453860526464-PT-440847244");

  }

  /**
   * The test inserts a few time partitioned datasets. A query is issued that retrieves the partitions older than 2
   * years.
   */
  @Test
  public void testKeepLast2YearsOfDailyPartitions() throws Exception {

    insertDailyPartition(new Path("/data/tracking/MetricEvent/daily/2015/11/25")); //61 days
    insertDailyPartition(new Path("/data/tracking/MetricEvent/daily/2015/12/01")); // 55 days
    insertDailyPartition(new Path("/data/tracking/MetricEvent/daily/2014/11/21")); // 430 days
    insertDailyPartition(new Path("/data/tracking/MetricEvent/daily/2014/01/22")); // 733 days (more than 2 years)
    insertDailyPartition(new Path("/data/tracking/MetricEvent/daily/2013/01/25")); // 1095 days (more than 2 years)

    // Use the current timestamp for consistent test results.
    Timestamp currentTimestamp =
        new Timestamp(DateTimeFormat.forPattern(DAILY_PARTITION_PATTERN).parseDateTime("2016/01/25").getMillis());

    PreparedStatement statement =
        connection.prepareStatement("SELECT path FROM Daily_Partitions WHERE TIMESTAMP_DIFF(?, ts, 'Days') > ?");
    statement.setTimestamp(1, currentTimestamp);
    statement.setLong(2, TWO_YEARS_IN_DAYS);
    ResultSet rs = statement.executeQuery();

    // Daily partitions to be cleaned
    rs.next();
    Assert.assertEquals(rs.getString(1), "/data/tracking/MetricEvent/daily/2014/01/22");
    rs.next();
    Assert.assertEquals(rs.getString(1), "/data/tracking/MetricEvent/daily/2013/01/25");

  }

  private void insertSnapshot(Path snapshotPath) throws Exception {

    String datasetPath = StringUtils.substringBeforeLast(snapshotPath.toString(), Path.SEPARATOR);
    String snapshotName = StringUtils.substringAfterLast(snapshotPath.toString(), Path.SEPARATOR);
    long ts = Long.parseLong(StringUtils.substringBefore(snapshotName, "-PT-"));
    long recordCount = Long.parseLong(StringUtils.substringAfter(snapshotName, "-PT-"));

    PreparedStatement insert = connection.prepareStatement("INSERT INTO Snapshots VALUES (?, ?, ?, ?, ?)");
    insert.setString(1, datasetPath);
    insert.setString(2, snapshotName);
    insert.setString(3, snapshotPath.toString());
    insert.setTimestamp(4, new Timestamp(ts));
    insert.setLong(5, recordCount);

    insert.executeUpdate();

  }

  private void insertDailyPartition(Path dailyPartitionPath) throws Exception {

    String datasetPath = StringUtils.substringBeforeLast(dailyPartitionPath.toString(), Path.SEPARATOR + "daily");

    DateTime partition =
        DateTimeFormat.forPattern(DAILY_PARTITION_PATTERN).parseDateTime(
            StringUtils.substringAfter(dailyPartitionPath.toString(), "daily" + Path.SEPARATOR));

    PreparedStatement insert = connection.prepareStatement("INSERT INTO Daily_Partitions VALUES (?, ?, ?)");
    insert.setString(1, datasetPath);
    insert.setString(2, dailyPartitionPath.toString());
    insert.setTimestamp(3, new Timestamp(partition.getMillis()));

    insert.executeUpdate();

  }

  private void execute(String query) throws SQLException {
    PreparedStatement insertStatement = connection.prepareStatement(query);
    insertStatement.executeUpdate();
  }
}
