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

package org.apache.gobblin.metastore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.io.Text;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.metastore.metadata.StateStoreEntryManager;
import org.apache.gobblin.metastore.predicates.StateStorePredicate;
import org.apache.gobblin.metastore.predicates.StoreNamePredicate;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.io.StreamUtils;
import org.apache.gobblin.util.jdbc.MysqlDataSourceUtils;

/**
 * An implementation of {@link StateStore} backed by MySQL.
 *
 * <p>
 *
 *     This implementation stores serialized {@link State}s as a blob in the database in the Sequence file format.
 *     The database table row is keyed by the store name and table name.
 *     State keys are state IDs (see {@link State#getId()}), and values are objects of {@link State} or
 *     any of its extensions. Keys will be empty strings if state IDs are not set
 *     (i.e., {@link State#getId()} returns <em>null</em>). In this case, the
 *     {@link MysqlStateStore#get(String, String, String)} method may not work.
 * </p>
 *
 * @param <T> state object type
 **/
public class MysqlStateStore<T extends State> implements StateStore<T> {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlStateStore.class);

  /** Specifies which 'Job State' query columns receive search evaluation (with SQL `LIKE` operator). */
  protected enum JobStateSearchColumns {
    NONE,
    TABLE_NAME_ONLY,
    STORE_NAME_AND_TABLE_NAME;
  }

  // Class of the state objects to be put into the store
  private final Class<T> stateClass;
  protected final DataSource dataSource;
  private final boolean compressedValues;

  private static final String UPSERT_JOB_STATE_TEMPLATE =
      "INSERT INTO $TABLE$ (store_name, table_name, state) VALUES(?,?,?)"
          + " ON DUPLICATE KEY UPDATE state = values(state)";

  private static final String SELECT_JOB_STATE_TEMPLATE =
      "SELECT state FROM $TABLE$ WHERE store_name = ? and table_name = ?";

  private static final String SELECT_JOB_STATE_WITH_LIKE_TEMPLATE =
      "SELECT state FROM $TABLE$ WHERE store_name = ? and table_name like ?";

  private static final String SELECT_JOB_STATE_WITH_BOTH_LIKES_TEMPLATE =
      "SELECT state FROM $TABLE$ WHERE store_name like ? and table_name like ?";

  private static final String SELECT_ALL_JOBS_STATE = "SELECT state FROM $TABLE$";

  private static final String SELECT_JOB_STATE_EXISTS_TEMPLATE =
      "SELECT 1 FROM $TABLE$ WHERE store_name = ? and table_name = ?";

  private static final String SELECT_JOB_STATE_NAMES_TEMPLATE =
      "SELECT table_name FROM $TABLE$ WHERE store_name = ?";

  private static final String SELECT_STORE_NAMES_TEMPLATE =
      "SELECT distinct store_name FROM $TABLE$";

  private static final String DELETE_JOB_STORE_TEMPLATE =
      "DELETE FROM $TABLE$ WHERE store_name = ?";

  private static final String DELETE_JOB_STATE_TEMPLATE =
      "DELETE FROM $TABLE$ WHERE store_name = ? AND table_name = ?";

  private static final String CLONE_JOB_STATE_TEMPLATE =
      "INSERT INTO $TABLE$(store_name, table_name, state)"
          + " (SELECT store_name, ?, state FROM $TABLE$ s WHERE"
          + " store_name = ? AND table_name = ?)"
          + " ON DUPLICATE KEY UPDATE state = s.state";

  private static final String SELECT_METADATA_TEMPLATE =
      "SELECT store_name, table_name, modified_time from $TABLE$ where store_name like ?";

  // MySQL key length limit is 767 bytes
  private static final String CREATE_JOB_STATE_TABLE_TEMPLATE =
      "CREATE TABLE IF NOT EXISTS $TABLE$ (store_name varchar(100) CHARACTER SET latin1 COLLATE latin1_bin not null,"
          + "table_name varchar(667) CHARACTER SET latin1 COLLATE latin1_bin not null,"
          + " modified_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
          + " state longblob, primary key(store_name, table_name))";

  private final String UPSERT_JOB_STATE_SQL;
  private final String SELECT_JOB_STATE_SQL;
  private final String SELECT_ALL_JOBS_STATE_SQL;
  private final String SELECT_JOB_STATE_WITH_LIKE_SQL;
  private final String SELECT_JOB_STATE_WITH_BOTH_LIKES_SQL;
  private final String SELECT_JOB_STATE_EXISTS_SQL;
  private final String SELECT_JOB_STATE_NAMES_SQL;
  private final String DELETE_JOB_STORE_SQL;
  private final String DELETE_JOB_STATE_SQL;
  private final String CLONE_JOB_STATE_SQL;
  private final String SELECT_STORE_NAMES_SQL;
  protected final String SELECT_METADATA_SQL;

  /**
   * Manages the persistence and retrieval of {@link State} in a MySQL database
   * @param dataSource the {@link DataSource} object for connecting to MySQL
   * @param stateStoreTableName the table for storing the state in rows keyed by two levels (store_name, table_name)
   * @param compressedValues should values be compressed for storage?
   * @param stateClass class of the {@link State}s stored in this state store
   * @throws IOException
   */
  public MysqlStateStore(DataSource dataSource, String stateStoreTableName, boolean compressedValues,
      Class<T> stateClass) throws IOException {
    this.dataSource = dataSource;
    this.stateClass = stateClass;
    this.compressedValues = compressedValues;

    UPSERT_JOB_STATE_SQL = UPSERT_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_SQL = SELECT_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_WITH_LIKE_SQL = SELECT_JOB_STATE_WITH_LIKE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_WITH_BOTH_LIKES_SQL = SELECT_JOB_STATE_WITH_BOTH_LIKES_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_ALL_JOBS_STATE_SQL = SELECT_ALL_JOBS_STATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_EXISTS_SQL = SELECT_JOB_STATE_EXISTS_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_NAMES_SQL = SELECT_JOB_STATE_NAMES_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    DELETE_JOB_STORE_SQL = DELETE_JOB_STORE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    DELETE_JOB_STATE_SQL = DELETE_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    CLONE_JOB_STATE_SQL = CLONE_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_STORE_NAMES_SQL = SELECT_STORE_NAMES_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_METADATA_SQL = SELECT_METADATA_TEMPLATE.replace("$TABLE$", stateStoreTableName);

    // create table if it does not exist
    String createJobTable = getCreateJobStateTableTemplate().replace("$TABLE$", stateStoreTableName);
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(createJobTable)) {
      createStatement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException("Failure creation table " + stateStoreTableName, e);
    }
  }

  protected String getCreateJobStateTableTemplate() {
    return CREATE_JOB_STATE_TABLE_TEMPLATE;
  }

  /**
   * creates a new {@link DataSource}
   * @param config the properties used for datasource instantiation
   * @return
   */
  public static DataSource newDataSource(Config config) {
    HikariDataSource dataSource = new HikariDataSource();
    PasswordManager passwordManager = PasswordManager.getInstance(ConfigUtils.configToProperties(config));

    dataSource.setDriverClassName(ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_DB_JDBC_DRIVER_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER));
    // MySQL server can timeout a connection so need to validate connections before use
    final String validationQuery = MysqlDataSourceUtils.QUERY_CONNECTION_IS_VALID_AND_NOT_READONLY;
    LOG.info("setting `DataSource` validation query: '" + validationQuery + "'");
    // TODO: revisit following verification of successful connection pool migration:
    //   If your driver supports JDBC4 we strongly recommend not setting this property. This is for "legacy" drivers
    //   that do not support the JDBC4 Connection.isValid() API; see:
    //   https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby
    dataSource.setConnectionTestQuery(validationQuery);
    dataSource.setAutoCommit(false);
    dataSource.setIdleTimeout(Duration.ofSeconds(60).toMillis());
    dataSource.setJdbcUrl(config.getString(ConfigurationKeys.STATE_STORE_DB_URL_KEY));
    // TODO: revisit following verification of successful connection pool migration:
    //   whereas `o.a.commons.dbcp.BasicDataSource` defaults min idle conns to 0, hikari defaults to 10.
    //   perhaps non-zero would have desirable runtime perf, but anything >0 currently fails unit tests (even 1!);
    //   (so experimenting with a higher number would first require adjusting tests)
    dataSource.setMinimumIdle(0);
    dataSource.setUsername(passwordManager.readPassword(
        config.getString(ConfigurationKeys.STATE_STORE_DB_USER_KEY)));
    dataSource.setPassword(passwordManager.readPassword(
        config.getString(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY)));

    return dataSource;
  }

  /**
   * return an identifier for the data source based on the configuration
   * @param config configuration
   * @return a {@link String} to identify the data source
   */
  public static String getDataSourceId(Config config) {
    PasswordManager passwordManager = PasswordManager.getInstance(ConfigUtils.configToProperties(config));

    return ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_DB_JDBC_DRIVER_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER) + "::"
        + config.getString(ConfigurationKeys.STATE_STORE_DB_URL_KEY) + "::"
        + passwordManager.readPassword(config.getString(ConfigurationKeys.STATE_STORE_DB_USER_KEY));
  }

  @Override
  public boolean create(String storeName) throws IOException {
    /* nothing to do since state will be stored as a new row in a DB table that has been validated */
    return true;
  }

  @Override
  public boolean create(String storeName, String tableName) throws IOException {
    if (exists(storeName, tableName)) {
      throw new IOException(String.format("State already exists for storeName %s tableName %s", storeName, tableName));
    }

    return true;
  }

  @Override
  public boolean exists(String storeName, String tableName) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_JOB_STATE_EXISTS_SQL)) {
      int index = 0;
      queryStatement.setString(++index, storeName);
      queryStatement.setString(++index, tableName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        if (rs.next()) {
          return true;
        } else {
          return false;
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failure checking existence of storeName " + storeName + " tableName " + tableName, e);
    }
  }

  /**
   * Serializes the state to the {@link DataOutput}
   * @param dataOutput output target receiving the serialized data
   * @param state the state to serialize
   * @throws IOException
   */
  private void addStateToDataOutputStream(DataOutput dataOutput, T state) throws IOException {
    new Text(Strings.nullToEmpty(state.getId())).write(dataOutput);
    state.write(dataOutput);
  }

  @Override
  public void put(String storeName, String tableName, T state) throws IOException {
    putAll(storeName, tableName, Collections.singleton(state));
  }

  @Override
  public void putAll(String storeName, String tableName, Collection<T> states) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement insertStatement = connection.prepareStatement(UPSERT_JOB_STATE_SQL);
        ByteArrayOutputStream byteArrayOs = new ByteArrayOutputStream();
        OutputStream os = compressedValues ? new GZIPOutputStream(byteArrayOs) : byteArrayOs;
        DataOutputStream dataOutput = new DataOutputStream(os)) {

      insertStatement.setString(1, storeName);
      insertStatement.setString(2, tableName);

      for (T state : states) {
        addStateToDataOutputStream(dataOutput, state);
      }

      dataOutput.close();
      insertStatement.setBlob(3, new ByteArrayInputStream(byteArrayOs.toByteArray()));

      insertStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("Failure storing state to store " + storeName + " table " + tableName, e);
    }
  }

  @Override
  public T get(String storeName, String tableName, String stateId) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_JOB_STATE_SQL)) {
      queryStatement.setString(1, storeName);
      queryStatement.setString(2, tableName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        if (rs.next()) {
          Blob blob = rs.getBlob(1);
          Text key = new Text();

          try (InputStream is = StreamUtils.isCompressed(blob.getBytes(1, 2)) ?
              new GZIPInputStream(blob.getBinaryStream()) : blob.getBinaryStream();
              DataInputStream dis = new DataInputStream(is)){
            // keep deserializing while we have data
            while (dis.available() > 0) {
              T state = this.stateClass.newInstance();

              key.readFields(dis);
              state.readFields(dis);
              state.setId(key.toString());
              if (key.toString().equals(stateId)) {
                return state;
              }
            }
          } catch (EOFException e) {
            // no more data. GZIPInputStream.available() doesn't return 0 until after EOF.
          }
        }
      }
    } catch (RuntimeException e) {
      throw e;
    }catch (Exception e) {
      throw new IOException("failure retrieving state from storeName " + storeName + " tableName " + tableName, e);
    }

    return null;
  }

  protected List<T> getAll(String storeName, String tableName, JobStateSearchColumns searchColumns) throws IOException {
    List<T> states = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(
            searchColumns == JobStateSearchColumns.TABLE_NAME_ONLY ?
                SELECT_JOB_STATE_WITH_LIKE_SQL :
                searchColumns == JobStateSearchColumns.STORE_NAME_AND_TABLE_NAME ?
                    SELECT_JOB_STATE_WITH_BOTH_LIKES_SQL :
                    SELECT_JOB_STATE_SQL)) {
      queryStatement.setString(1, storeName);
      queryStatement.setString(2, tableName);
      execGetAllStatement(queryStatement, states);
      return states;
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new IOException("failure retrieving state from storeName " + storeName + " tableName " + tableName, e);
    }
  }

  /**
   * An additional {@link #getAll()} method to retrieve all entries in a table.
   *
   */
  public List<T> getAll() throws IOException {
    List<T> states = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_ALL_JOBS_STATE_SQL)) {
      execGetAllStatement(queryStatement, states);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new IOException(String.format("failure retrieving all states with the SQL[%s]", SELECT_ALL_JOBS_STATE_SQL), e);
    }
    return states;
  }

  @Override
  public List<T> getAll(String storeName, String tableName) throws IOException {
    return getAll(storeName, tableName, JobStateSearchColumns.NONE);
  }

  @Override
  public List<T> getAll(String storeName) throws IOException {
    return getAll(storeName, "%", JobStateSearchColumns.TABLE_NAME_ONLY);
  }

  /**
   * An helper function extracted from getAll method originally that has side effects:
   * - Executing queryStatement
   * - Put the result into List<state> object.
   * @throws SQLException
   * @throws Exception
   */
  private void execGetAllStatement(PreparedStatement queryStatement, List<T> states) throws SQLException, Exception {
    try (ResultSet rs = queryStatement.executeQuery()) {
      while (rs.next()) {
        Blob blob = rs.getBlob(1);
        Text key = new Text();

        try (InputStream is = StreamUtils.isCompressed(blob.getBytes(1, 2)) ?
            new GZIPInputStream(blob.getBinaryStream()) : blob.getBinaryStream();
            DataInputStream dis = new DataInputStream(is)) {
          // keep deserializing while we have data
          while (dis.available() > 0) {
            T state = this.stateClass.newInstance();
            String stateId = key.readString(dis);
            state.readFields(dis);
            state.setId(stateId);
            states.add(state);
          }
        } catch (EOFException e) {
          // no more data. GZIPInputStream.available() doesn't return 0 until after EOF.
        }
      }
    }
  }

  @Override
  public List<String> getTableNames(String storeName, Predicate<String> predicate) throws IOException {
    List<String> names = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_JOB_STATE_NAMES_SQL)) {
      queryStatement.setString(1, storeName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          String name = rs.getString(1);
          if (predicate.apply(name)) {
            names.add(name);
          }
        }
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Could not query table names for store %s", storeName), e);
    }

    return names;
  }

  /**
   * Get store names in the state store
   *
   * @param predicate only returns names matching predicate
   * @return (possibly empty) list of store names from the given store
   * @throws IOException
   */
  public List<String> getStoreNames(Predicate<String> predicate)
      throws IOException {
    List<String> names = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_STORE_NAMES_SQL)) {

      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          String name = rs.getString(1);
          if (predicate.apply(name)) {
            names.add(name);
          }
        }
      }
    } catch (SQLException e) {
      throw new IOException(String.format("Could not query store names"), e);
    }

    return names;
  }


  @Override
  public void createAlias(String storeName, String original, String alias) throws IOException {

    if (!exists(storeName, original)) {
      throw new IOException(String.format("State does not exist for table %s", original));
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement cloneStatement = connection.prepareStatement(CLONE_JOB_STATE_SQL)) {
      int index = 0;
      cloneStatement.setString(++index, alias);
      cloneStatement.setString(++index, storeName);
      cloneStatement.setString(++index, original);
      cloneStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException(String.format("Failure creating alias for store %s original %s", storeName, original), e);
    }
  }

  @Override
  public void delete(String storeName, String tableName) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement deleteStatement = connection.prepareStatement(DELETE_JOB_STATE_SQL)) {
      int index = 0;
      deleteStatement.setString(++index, storeName);
      deleteStatement.setString(++index, tableName);
      deleteStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("failure deleting storeName " + storeName + " tableName " + tableName, e);
    }
  }

  @Override
  public void delete(String storeName) throws IOException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement deleteStatement = connection.prepareStatement(DELETE_JOB_STORE_SQL)) {
      deleteStatement.setString(1, storeName);
      deleteStatement.executeUpdate();
      connection.commit();
    } catch (SQLException e) {
      throw new IOException("failure deleting storeName " + storeName, e);
    }
  }

  /**
   * Gets entry managers for all tables matching the predicate
   * @param predicate Predicate used to filter tables. To allow state stores to push down predicates, use native extensions
   *                  of {@link StateStorePredicate}.
   * @throws IOException
   */
  @Override
  public List<? extends StateStoreEntryManager> getMetadataForTables(StateStorePredicate predicate)
      throws IOException {
    List<MysqlStateStoreEntryManager> entryManagers = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(SELECT_METADATA_SQL)) {
      String storeName = predicate instanceof StoreNamePredicate ? ((StoreNamePredicate) predicate).getStoreName() : "%";
      queryStatement.setString(1, storeName);

      try (ResultSet rs = queryStatement.executeQuery()) {
        while (rs.next()) {
          String rsStoreName = rs.getString(1);
          String rsTableName = rs.getString(2);
          Timestamp timestamp = rs.getTimestamp(3);

          StateStoreEntryManager entryManager =
              new MysqlStateStoreEntryManager(rsStoreName, rsTableName, timestamp.getTime(), this);

          if (predicate.apply(entryManager)) {
            entryManagers.add(new MysqlStateStoreEntryManager(rsStoreName, rsTableName, timestamp.getTime(), this));
          }
        }
      }
    } catch (SQLException e) {
      throw new IOException("failure getting metadata for tables", e);
    }

    return entryManagers;
  }

  /**
   * For setting timestamps in tests
   * @param timestamp 0 to set to default, non-zero to set an epoch time
   * @throws SQLException
   */
  @VisibleForTesting
  public void setTestTimestamp(long timestamp) throws IOException {
    String statement = "SET TIMESTAMP =";

    // 0 is used to reset to the default
    if (timestamp > 0 ) {
      statement += timestamp;
    } else {
      statement += " DEFAULT";
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement =
            connection.prepareStatement(statement)) {
      queryStatement.execute();
    } catch (SQLException e) {
      throw new IOException("Could not set timestamp " + timestamp, e);
    }
  }
}
