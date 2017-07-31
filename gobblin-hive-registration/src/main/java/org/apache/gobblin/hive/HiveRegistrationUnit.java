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

package org.apache.gobblin.hive;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;


/**
 * A class that represents a Hive table or partition.
 *
 * @author Ziyang Liu
 */
@Getter
@Alpha
@ToString
public class HiveRegistrationUnit {

  protected final String dbName;
  protected final String tableName;
  protected final List<Column> columns = Lists.newArrayList();
  protected final State props = new State();
  protected final State storageProps = new State();
  protected final State serDeProps = new State();

  protected final Optional<HiveSerDeManager> serDeManager;

  /**
   * Table or Partition properties
   */
  protected Optional<Long> createTime;
  protected Optional<Long> lastAccessTime;

  /**
   * Storage properties
   */
  protected Optional<String> location;
  protected Optional<String> inputFormat;
  protected Optional<String> outputFormat;
  protected Optional<Boolean> isCompressed;
  protected Optional<Integer> numBuckets;
  protected Optional<List<String>> bucketColumns;
  protected Optional<Boolean> isStoredAsSubDirs;

  /**
   * SerDe properties
   */
  protected Optional<String> serDeType;

  HiveRegistrationUnit(Builder<?> builder) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(builder.dbName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(builder.tableName));

    this.dbName = builder.dbName;
    this.tableName = builder.tableName;
    this.columns.addAll(builder.columns);
    this.props.addAll(builder.props);
    this.storageProps.addAll(builder.storageProps);
    this.serDeProps.addAll(builder.serDeProps);

    this.serDeManager = builder.serDeManager;

    populateTablePartitionFields(this.props);
    populateStorageFields(this.storageProps);
    populateSerDeFields(this.serDeProps);
  }

  @SuppressWarnings("serial")
  protected void populateTablePartitionFields(State state) {
    this.createTime = populateField(state, HiveConstants.CREATE_TIME, new TypeToken<Long>() {});
    this.lastAccessTime = populateField(state, HiveConstants.LAST_ACCESS_TIME, new TypeToken<Long>() {});
  }

  @SuppressWarnings({ "serial" })
  protected void populateStorageFields(State state) {
    this.location = populateField(state, HiveConstants.LOCATION, new TypeToken<String>() {});
    this.inputFormat = populateField(state, HiveConstants.INPUT_FORMAT, new TypeToken<String>() {});
    this.outputFormat = populateField(state, HiveConstants.OUTPUT_FORMAT, new TypeToken<String>() {});
    this.isCompressed = populateField(state, HiveConstants.COMPRESSED, new TypeToken<Boolean>() {});
    this.numBuckets = populateField(state, HiveConstants.NUM_BUCKETS, new TypeToken<Integer>() {});
    this.bucketColumns = populateField(state, HiveConstants.BUCKET_COLUMNS, new TypeToken<List<String>>() {});
    this.isStoredAsSubDirs = populateField(state, HiveConstants.STORED_AS_SUB_DIRS, new TypeToken<Boolean>() {});
  }

  @SuppressWarnings("serial")
  protected void populateSerDeFields(State state) {
    this.serDeType = populateField(state, HiveConstants.SERDE_TYPE, new TypeToken<String>() {});
  }

  @SuppressWarnings({ "serial", "unchecked" })
  protected static <T> Optional<T> populateField(State state, String key, TypeToken<T> token) {
    if (state.contains(key)) {
      Optional<T> fieldValue;

      if (new TypeToken<Boolean>() {}.isAssignableFrom(token)) {
        fieldValue = (Optional<T>) Optional.of(state.getPropAsBoolean(key));
      } else if (new TypeToken<Integer>() {}.isAssignableFrom(token)) {
        fieldValue = (Optional<T>) Optional.of(state.getPropAsInt(key));
      } else if (new TypeToken<Long>() {}.isAssignableFrom(token)) {
        fieldValue = (Optional<T>) Optional.of(state.getPropAsLong(key));
      } else if (new TypeToken<List<String>>() {}.isAssignableFrom(token)) {
        fieldValue = (Optional<T>) Optional.of(state.getPropAsList(key));
      } else {
        fieldValue = (Optional<T>) Optional.of(state.getProp(key));
      }

      state.removeProp(key);
      return fieldValue;
    }
    return Optional.<T> absent();
  }

  /**
   * Set the columns for a table or partition.
   *
   * <p>
   *   Columns does not need to be set for a table if the table's serde already provides the schema,
   *   such as Avro tables. Columns does not need to be set for a partition if they are the same as
   *   the table's columns.
   * </p>
   * @param columns
   */
  public void setColumns(List<Column> columns) {
    this.columns.clear();
    this.columns.addAll(columns);
  }

  /**
   * Set a table/partition parameter.
   *
   * <p>
   *   When using {@link org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProp(String, Object)}, {@link #setStorageProp(String, Object)} and
   *   {@link #setSerDeProp(String, Object)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setProp(String key, Object value) {
    this.props.setProp(key, value);
    updateTablePartitionFields(this.props, key, value);
  }

  /**
   * Set a storage parameter for a table/partition.
   *
   * <p>
   *   When using {@link org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProp(String, Object)}, {@link #setStorageProp(String, Object)} and
   *   {@link #setSerDeProp(String, Object)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setStorageProp(String key, Object value) {
    this.storageProps.setProp(key, value);
    updateStorageFields(this.storageProps, key, value);
  }

  /**
   * Set a serde parameter for a table/partition.
   *
   * <p>
   *   When using {@link org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProp(String, Object)}, {@link #setStorageProp(String, Object)} and
   *   {@link #setSerDeProp(String, Object)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setSerDeProp(String key, Object value) {
    this.serDeProps.setProp(key, value);
    updateSerDeFields(this.serDeProps, key, value);
  }

  /**
   * Set table/partition parameters.
   *
   * <p>
   *   When using {@link org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProps(State)}, {@link #setStorageProps(State)} and
   *   {@link #setSerDeProps(State)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setProps(State props) {
    for (String propKey : props.getPropertyNames()) {
      setProp(propKey, props.getProp(propKey));
    }
  }

  /**
   * Set storage parameters for a table/partition.
   *
   * <p>
   *   When using {@link org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProps(State)}, {@link #setStorageProps(State)} and
   *   {@link #setSerDeProps(State)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setStorageProps(State storageProps) {
    for (String propKey : storageProps.getPropertyNames()) {
      setStorageProp(propKey, storageProps.getProp(propKey));
    }
  }

  /**
   * Set serde parameters for a table/partition.
   *
   * <p>
   *   When using {@link org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister}, since it internally use
   *   {@link org.apache.hadoop.hive.metastore.api.Table} and {@link org.apache.hadoop.hive.metastore.api.Partition}
   *   which distinguishes between table/partition parameters, storage descriptor parameters, and serde parameters,
   *   one may need to distinguish them when constructing a {@link HiveRegistrationUnit} by using
   *   {@link #setProps(State)}, {@link #setStorageProps(State)} and
   *   {@link #setSerDeProps(State)}. When using query-based Hive registration, they do not need to be
   *   distinguished since all parameters will be passed via TBLPROPERTIES.
   * </p>
   */
  public void setSerDeProps(State serdeProps) {
    for (String propKey : serdeProps.getPropertyNames()) {
      setSerDeProp(propKey, serdeProps.getProp(propKey));
    }
  }

  protected void updateTablePartitionFields(State state, String key, Object value) {
    boolean isExistingField = true;
    switch (key) {
      case HiveConstants.CREATE_TIME:
        this.createTime = Optional.of((Long) value);
        break;
      case HiveConstants.LAST_ACCESS_TIME:
        this.createTime = Optional.of((Long) value);
        break;
      default:
        isExistingField = false;
    }
    if (isExistingField) {
      state.removeProp(key);
    }
  }

  protected void updateStorageFields(State state, String key, Object value) {
    boolean isExistingField = true;
    switch (key) {
      case HiveConstants.LOCATION:
        this.location = Optional.of((String) value);
        break;
      case HiveConstants.INPUT_FORMAT:
        this.inputFormat = Optional.of((String) value);
        break;
      case HiveConstants.OUTPUT_FORMAT:
        this.outputFormat = Optional.of((String) value);
        break;
      case HiveConstants.COMPRESSED:
        this.isCompressed = Optional.of((Boolean) value);
        break;
      case HiveConstants.NUM_BUCKETS:
        this.numBuckets = Optional.of((Integer) value);
        break;
      case HiveConstants.BUCKET_COLUMNS:
        this.bucketColumns = Optional.of(Splitter.on(',').omitEmptyStrings().trimResults().splitToList((String) value));
        break;
      case HiveConstants.STORED_AS_SUB_DIRS:
        this.isStoredAsSubDirs = Optional.of((Boolean) value);
        break;
      default:
        isExistingField = false;
    }
    if (isExistingField) {
      state.removeProp(key);
    }
  }

  protected void updateSerDeFields(State state, String key, Object value) {
    boolean isExistingField = true;
    switch (key) {
      case HiveConstants.SERDE_TYPE:
        this.serDeType = Optional.of((String) value);
        break;
      default:
        isExistingField = false;
    }
    if (isExistingField) {
      state.removeProp(key);
    }
  }

  /**
   * Set serde properties for a table/partition using the table/partition's {@link HiveSerDeManager}.
   *
   * <p>
   *   Requires that the {@link HiveSerDeManager} of the table/partition must be specified in
   *   {@link Builder#withSerdeManaager(HiveSerDeManager)}, and the table/partition's location must be specified
   *   either in {@link #setLocation(String)} or via {@link HiveConstants#LOCATION}.
   * </p>
   */
  public void setSerDeProps(Path path) throws IOException {
    this.serDeManager.get().addSerDeProperties(path, this);
  }

  /**
   * Set serde properties for a table/partition using another table/partition's serde properties.
   *
   * <p>
   *   A benefit of doing this is to avoid obtaining the schema multiple times when creating a table and a partition
   *   with the same schema, or creating several tables and partitions with the same schema. After the first
   *   table/partition is created, one can use the same SerDe properties to create the other tables/partitions.
   * </p>
   */
  public void setSerDeProps(HiveRegistrationUnit other) throws IOException {
    this.serDeManager.get().addSerDeProperties(other, this);
  }

  public void setCreateTime(long createTime) {
    this.createTime = Optional.of(createTime);
  }

  public void setLastAccessTime(long lastAccessTime) {
    this.lastAccessTime = Optional.of(lastAccessTime);
  }

  public void setLocation(String location) {
    this.location = Optional.of(location);
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = Optional.of(inputFormat);
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = Optional.of(outputFormat);
  }

  public void setCompressed(boolean isCompressed) {
    this.isCompressed = Optional.of(isCompressed);
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = Optional.of(numBuckets);
  }

  public void setBucketColumns(List<String> bucketColumns) {
    this.bucketColumns = Optional.<List<String>> of(ImmutableList.<String> copyOf(bucketColumns));
  }

  public void setStoredAsSubDirs(boolean isStoredAsSubDirs) {
    this.isStoredAsSubDirs = Optional.of(isStoredAsSubDirs);
  }

  public void setSerDeType(String serDeType) {
    this.serDeType = Optional.of(serDeType);
  }

  static abstract class Builder<T extends Builder<?>> {
    private String dbName;
    private String tableName;
    private List<Column> columns = Lists.newArrayList();
    private State props = new State();
    private State storageProps = new State();
    private State serDeProps = new State();
    private Optional<HiveSerDeManager> serDeManager = Optional.absent();

    @SuppressWarnings("unchecked")
    public T withDbName(String dbName) {
      this.dbName = dbName;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withTableName(String tableName) {
      this.tableName = tableName;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withColumns(List<Column> columns) {
      this.columns = columns;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withProps(State props) {
      this.props = props;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withStorageProps(State storageProps) {
      this.storageProps = storageProps;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withSerdeProps(State serDeProps) {
      this.serDeProps = serDeProps;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T withSerdeManaager(HiveSerDeManager serDeManager) {
      this.serDeManager = Optional.of(serDeManager);
      return (T) this;
    }

    public abstract HiveRegistrationUnit build();
  }

  @AllArgsConstructor
  @Getter
  public static class Column {

    private final String name;
    private final String type;
    private final String comment;
  }
}
