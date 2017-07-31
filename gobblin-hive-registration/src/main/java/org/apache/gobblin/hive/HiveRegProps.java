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

import org.apache.gobblin.hive.metastore.HiveMetaStoreUtils;
import java.util.List;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import lombok.EqualsAndHashCode;
import lombok.Getter;

import org.apache.gobblin.annotation.Alpha;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.hive.metastore.HiveMetaStoreBasedRegister;


/**
 * An extension to {@link State} for Hive registration.
 *
 * @author Ziyang Liu
 */
@Alpha
@Getter
@EqualsAndHashCode(callSuper = true)
public class HiveRegProps extends State {

  public static final String HIVE_DB_ROOT_DIR = "hive.db.root.dir";
  public static final String HIVE_REGISTER_THREADS = "hive.register.threads";
  public static final int DEFAULT_HIVE_REGISTER_THREADS = 20;
  public static final String HIVE_TABLE_PARTITION_PROPS = "hive.table.partition.props";
  public static final String HIVE_STORAGE_PROPS = "hive.storage.props";
  public static final String HIVE_SERDE_PROPS = "hive.serde.props";
  public static final String HIVE_UPSTREAM_DATA_ATTR_NAMES= "hive.upstream.data.attr.names";

  private static final Splitter SPLITTER = Splitter.on(':').trimResults().omitEmptyStrings();

  private final State tablePartitionProps;
  private final State storageProps;
  private final State serdeProps;

  private Optional<String> runtimeTableProps;

  /**
   * @param props A {@link State} object that includes both properties required by {@link HiveMetaStoreBasedRegister} to do
   * Hive registration, as well as the Hive properties that will be added to the Hive table when creating the table,
   * e.g., orc.compress=SNAPPY
   *
   * <p>
   *   The Hive table properties should be a comma-separated list associated with {@link #HIVE_TABLE_PARTITION_PROPS} in the
   *   given {@link State}.
   * </p>
   */
  public HiveRegProps(State props) {
    super(props);
    this.tablePartitionProps = createHiveProps(HIVE_TABLE_PARTITION_PROPS);
    if (props.contains(HiveMetaStoreUtils.RUNTIME_PROPS)) {
      runtimeTableProps = Optional.of(props.getProp(HiveMetaStoreUtils.RUNTIME_PROPS));
    }
    else{
      runtimeTableProps = Optional.absent();
    }
    this.storageProps = createHiveProps(HIVE_STORAGE_PROPS);
    this.serdeProps = createHiveProps(HIVE_SERDE_PROPS);
  }

  /**
   * @param props Properties required by {@link HiveMetaStoreBasedRegister} to do Hive registration
   * @param tableProps Hive properties that will be added to the Hive table when creating the table,
   * e.g., orc.compress=SNAPPY
   */
  public HiveRegProps(State props, State tableProps, State storageProps, State serdeProps) {
    super(props);
    this.tablePartitionProps = tableProps;
    if (props.contains(HiveMetaStoreUtils.RUNTIME_PROPS)) {
      runtimeTableProps = Optional.of(props.getProp(HiveMetaStoreUtils.RUNTIME_PROPS));
    }
    else{
      runtimeTableProps = Optional.absent();
    }
    this.storageProps = storageProps;
    this.serdeProps = serdeProps;
  }

  /**
   * Create a {@link State} object that contains Hive table properties. These properties are obtained from
   * {@link #HIVE_TABLE_PARTITION_PROPS}, which is a list of comma-separated properties. Each property is in the form
   * of '[key]=[value]'.
   */
  private State createHiveProps(String propKey) {
    State state = new State();

    if (!contains(propKey)) {
      return state;
    }

    for (String propValue : getPropAsList(propKey)) {
      List<String> tokens = SPLITTER.splitToList(propValue);

      Preconditions.checkState(tokens.size() == 2, propValue + " is not a valid Hive table/partition property");
      state.setProp(tokens.get(0), tokens.get(1));
    }
    return state;
  }

  /**
   * Get Hive database root dir from {@link #HIVE_DB_ROOT_DIR}.
   *
   * @return {@link Optional#absent()} if {@link #HIVE_DB_ROOT_DIR} is not specified.
   */
  public Optional<String> getDbRootDir() {
    return Optional.fromNullable(getProp(HIVE_DB_ROOT_DIR));
  }

  /**
   * Get the name of registered HiveTable's upstream data attributes.
   * E.g., When data consumed from Kafka is registered into Hive Table, it is expected
   * to have Hive Metadata indicating the Kafka topic.
   *
   * HIVE_UPSTREAM_DATA_ATTR_NAMES is comma separated string, each item representing a upstream data attr.
   * E.g. hive.upstream.data.attr.names=topic.name,some.else
   *
   * @return {@link Optional#absent()} if {@link #HIVE_UPSTREAM_DATA_ATTR_NAMES} is not specified.
   */
  public Optional<String> getUpstreamDataAttrName(){
    return Optional.fromNullable(getProp(HIVE_UPSTREAM_DATA_ATTR_NAMES));
  }

  /**
   * Get number of threads from {@link #HIVE_REGISTER_THREADS}, with a default value of
   * {@link #DEFAULT_HIVE_REGISTER_THREADS}.
   */
  public int getNumThreads() {
    return getPropAsInt(HIVE_REGISTER_THREADS, DEFAULT_HIVE_REGISTER_THREADS);
  }
}
