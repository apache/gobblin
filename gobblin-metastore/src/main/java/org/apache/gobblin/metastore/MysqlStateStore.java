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

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.io.StreamUtils;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.io.Text;

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

  // Class of the state objects to be put into the store
  private final Class<T> stateClass;
  private final DataSource dataSource;
  private final boolean compressedValues;

  private static final String UPSERT_JOB_STATE_TEMPLATE =
      "INSERT INTO $TABLE$ (store_name, table_name, state) VALUES(?,?,?)"
          + " ON DUPLICATE KEY UPDATE state = values(state)";

  private static final String SELECT_JOB_STATE_TEMPLATE =
      "SELECT state FROM $TABLE$ WHERE store_name = ? and table_name = ?";

  private static final String SELECT_JOB_STATE_WITH_LIKE_TEMPLATE =
      "SELECT state FROM $TABLE$ WHERE store_name = ? and table_name like ?";

  private static final String SELECT_JOB_STATE_EXISTS_TEMPLATE =
      "SELECT 1 FROM $TABLE$ WHERE store_name = ? and table_name = ?";

  private static final String SELECT_JOB_STATE_NAMES_TEMPLATE =
      "SELECT table_name FROM $TABLE$ WHERE store_name = ?";

  private static final String DELETE_JOB_STORE_TEMPLATE =
      "DELETE FROM $TABLE$ WHERE store_name = ?";

  private static final String DELETE_JOB_STATE_TEMPLATE =
      "DELETE FROM $TABLE$ WHERE store_name = ? AND table_name = ?";

  private static final String CLONE_JOB_STATE_TEMPLATE =
      "INSERT INTO $TABLE$(store_name, table_name, state)"
          + " (SELECT store_name, ?, state FROM $TABLE$ s WHERE"
          + " store_name = ? AND table_name = ?)"
          + " ON DUPLICATE KEY UPDATE state = s.state";

  // MySQL key length limit is 767 bytes
  private static final String CREATE_JOB_STATE_TABLE_TEMPLATE =
      "CREATE TABLE IF NOT EXISTS $TABLE$ (store_name varchar(100) CHARACTER SET latin1 not null,"
          + "table_name varchar(667) CHARACTER SET latin1 not null,"
          + " modified_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
          + " state longblob, primary key(store_name, table_name))";

  private final String UPSERT_JOB_STATE_SQL;
  private final String SELECT_JOB_STATE_SQL;
  private final String SELECT_JOB_STATE_WITH_LIKE_SQL;
  private final String SELECT_JOB_STATE_EXISTS_SQL;
  private final String SELECT_JOB_STATE_NAMES_SQL;
  private final String DELETE_JOB_STORE_SQL;
  private final String DELETE_JOB_STATE_SQL;
  private final String CLONE_JOB_STATE_SQL;

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
    SELECT_JOB_STATE_EXISTS_SQL = SELECT_JOB_STATE_EXISTS_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    SELECT_JOB_STATE_NAMES_SQL = SELECT_JOB_STATE_NAMES_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    DELETE_JOB_STORE_SQL = DELETE_JOB_STORE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    DELETE_JOB_STATE_SQL = DELETE_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    CLONE_JOB_STATE_SQL = CLONE_JOB_STATE_TEMPLATE.replace("$TABLE$", stateStoreTableName);

    // create table if it does not exist
    String createJobTable = CREATE_JOB_STATE_TABLE_TEMPLATE.replace("$TABLE$", stateStoreTableName);
    try (Connection connection = dataSource.getConnection();
        PreparedStatement createStatement = connection.prepareStatement(createJobTable)) {
      createStatement.executeUpdate();
    } catch (SQLException e) {
      throw new IOException("Failure creation table " + stateStoreTableName, e);
    }
  }

  /**
   * creates a new {@link BasicDataSource}
   * @param config the properties used for datasource instantiation
   * @return
   */
  public static BasicDataSource newDataSource(Config config) {
    BasicDataSource basicDataSource = new BasicDataSource();
    PasswordManager passwordManager = PasswordManager.getInstance(ConfigUtils.configToProperties(config));

    basicDataSource.setDriverClassName(ConfigUtils.getString(config, ConfigurationKeys.STATE_STORE_DB_JDBC_DRIVER_KEY,
        ConfigurationKeys.DEFAULT_STATE_STORE_DB_JDBC_DRIVER));
    // MySQL server can timeout a connection so need to validate connections before use
    basicDataSource.setValidationQuery("select 1");
    basicDataSource.setTestOnBorrow(true);
    basicDataSource.setDefaultAutoCommit(false);
    basicDataSource.setTimeBetweenEvictionRunsMillis(60000);
    basicDataSource.setUrl(config.getString(ConfigurationKeys.STATE_STORE_DB_URL_KEY));
    basicDataSource.setUsername(passwordManager.readPassword(
        config.getString(ConfigurationKeys.STATE_STORE_DB_USER_KEY)));
    basicDataSource.setPassword(passwordManager.readPassword(
        config.getString(ConfigurationKeys.STATE_STORE_DB_PASSWORD_KEY)));
    basicDataSource.setMinEvictableIdleTimeMillis(
        ConfigUtils.getLong(config, ConfigurationKeys.STATE_STORE_DB_CONN_MIN_EVICTABLE_IDLE_TIME_KEY,
            ConfigurationKeys.DEFAULT_STATE_STORE_DB_CONN_MIN_EVICTABLE_IDLE_TIME));

    return basicDataSource;
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

      int index = 0;
      insertStatement.setString(++index, storeName);
      insertStatement.setString(++index, tableName);

      for (T state : states) {
        addStateToDataOutputStream(dataOutput, state);
      }

      dataOutput.close();
      insertStatement.setBlob(++index, new ByteArrayInputStream(byteArrayOs.toByteArray()));

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
      int index = 0;
      queryStatement.setString(++index, storeName);
      queryStatement.setString(++index, tableName);

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

  protected List<T> getAll(String storeName, String tableName, boolean useLike) throws IOException {
    List<T> states = Lists.newArrayList();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement queryStatement = connection.prepareStatement(useLike ?
            SELECT_JOB_STATE_WITH_LIKE_SQL : SELECT_JOB_STATE_SQL)) {
      queryStatement.setString(1, storeName);
      queryStatement.setString(2, tableName);

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
              key.readString(dis);
              state.readFields(dis);
              states.add(state);
            }
          } catch (EOFException e) {
            // no more data. GZIPInputStream.available() doesn't return 0 until after EOF.
          }
        }
      }
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new IOException("failure retrieving state from storeName " + storeName + " tableName " + tableName, e);
    }

    return states;
  }

  @Override
  public List<T> getAll(String storeName, String tableName) throws IOException {
    return getAll(storeName, tableName, false);
  }

  @Override
  public List<T> getAll(String storeName) throws IOException {
    return getAll(storeName, "%", true);
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
}
