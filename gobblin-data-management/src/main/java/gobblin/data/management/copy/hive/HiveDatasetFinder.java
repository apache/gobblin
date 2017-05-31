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

package gobblin.data.management.copy.hive;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.ConfigClientCache;
import gobblin.config.client.ConfigClientUtils;
import gobblin.config.client.api.ConfigStoreFactoryDoesNotExistsException;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.config.store.api.ConfigStoreCreationException;
import gobblin.configuration.ConfigurationKeys;
import gobblin.data.management.hive.HiveConfigClientUtils;
import gobblin.dataset.IterableDatasetFinder;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.util.AutoReturnableObject;
import gobblin.util.ConfigUtils;


/**
 * Finds {@link HiveDataset}s. Will look for tables in a database using a {@link WhitelistBlacklist},
 * and creates a {@link HiveDataset} for each one.
 */
@Slf4j
public class HiveDatasetFinder implements IterableDatasetFinder<HiveDataset> {

  public static final String HIVE_DATASET_PREFIX = "hive.dataset";
  public static final String HIVE_METASTORE_URI_KEY = HIVE_DATASET_PREFIX + ".hive.metastore.uri";
  public static final String DB_KEY = HIVE_DATASET_PREFIX + ".database";
  public static final String TABLE_PATTERN_KEY = HIVE_DATASET_PREFIX + ".table.pattern";
  public static final String DEFAULT_TABLE_PATTERN = "*";

  /*
   * By setting the prefix, only config keys with this prefix will be used to build a HiveDataset.
   * By passing scoped configurations the same config keys can be used in different contexts.
   *
   * E.g
   * 1. For CopySource, prefix is gobblin.dataset.copy
   * 2. For avro to Orc conversion, prefix is hive.dataset.conversion.avro.orc
   * 3. For retention, prefix is gobblin.retention.
   *
   */
  public static final String HIVE_DATASET_CONFIG_PREFIX_KEY = "hive.dataset.configPrefix";
  private static final String DEFAULT_HIVE_DATASET_CONIFG_PREFIX = StringUtils.EMPTY;

  public static final String HIVE_DATASET_IS_BLACKLISTED_KEY = "is.blacklisted";
  private static final boolean DEFAULT_HIVE_DATASET_IS_BLACKLISTED_KEY = false;

  /**
   * This is an optional key.
   * The fully qualified name of a {@link Function} class which returns the relative uri of a dataset in the config store
   */
  public static final String CONFIG_STORE_DATASET_URI_BUILDER_CLASS = "gobblin.config.management.datasetUriBuilderClass";

  // Event names
  private static final String DATASET_FOUND = "DatasetFound";
  private static final String DATASET_ERROR = "DatasetError";
  private static final String FAILURE_CONTEXT = "FailureContext";

  protected final Properties properties;
  protected final HiveMetastoreClientPool clientPool;
  protected final FileSystem fs;
  private final WhitelistBlacklist whitelistBlacklist;
  private final Optional<EventSubmitter> eventSubmitter;

  protected Optional<String> configStoreUri;
  protected final Function<Table, String> configStoreDatasetUriBuilder;

  protected final String datasetConfigPrefix;
  protected final ConfigClient configClient;
  private final Config jobConfig;

  public HiveDatasetFinder(FileSystem fs, Properties properties) throws IOException {
    this(fs, properties, createClientPool(properties));
  }

  protected HiveDatasetFinder(FileSystem fs, Properties properties, ConfigClient configClient) throws IOException {
    this(fs, properties, createClientPool(properties), null, configClient);
  }

  public HiveDatasetFinder(FileSystem fs, Properties properties, EventSubmitter eventSubmitter) throws IOException {
    this(fs, properties, createClientPool(properties), eventSubmitter);
  }

  protected HiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool clientPool)
      throws IOException {
    this(fs, properties, clientPool, null);
  }

  protected HiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool clientPool,
      EventSubmitter eventSubmitter) throws IOException {
    this(fs, properties, clientPool, eventSubmitter, ConfigClientCache.getClient(VersionStabilityPolicy.STRONG_LOCAL_STABILITY));
  }

  @SuppressWarnings("unchecked")
  //SupressWarning justification : CONFIG_STORE_DATASET_URI_BUILDER_CLASS must be of type Function<DbAndTable, String>.
  //It is safe to throw RuntimeException otherwise
  protected HiveDatasetFinder(FileSystem fs, Properties properties, HiveMetastoreClientPool clientPool,
      EventSubmitter eventSubmitter, ConfigClient configClient) throws IOException {

    this.properties = properties;
    this.clientPool = clientPool;
    this.fs = fs;

    String whitelistKey = HIVE_DATASET_PREFIX + "." + WhitelistBlacklist.WHITELIST;
    Preconditions.checkArgument(properties.containsKey(DB_KEY) || properties.containsKey(whitelistKey),
        String.format("Must specify %s or %s.", DB_KEY, whitelistKey));

    Config config = ConfigFactory.parseProperties(properties);

    if (properties.containsKey(DB_KEY)) {
      this.whitelistBlacklist = new WhitelistBlacklist(this.properties.getProperty(DB_KEY) + "."
          + this.properties.getProperty(TABLE_PATTERN_KEY, DEFAULT_TABLE_PATTERN), "");
    } else {
      this.whitelistBlacklist = new WhitelistBlacklist(config.getConfig(HIVE_DATASET_PREFIX));
    }

    this.eventSubmitter = Optional.fromNullable(eventSubmitter);
    this.configStoreUri = StringUtils.isNotBlank(properties.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI)) ?
        Optional.of(properties.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI)) : Optional.<String>absent();
    if (!Boolean.valueOf(properties.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_ENABLED,
        ConfigurationKeys.DEFAULT_CONFIG_MANAGEMENT_STORE_ENABLED))) {
      this.configStoreUri = Optional.<String>absent();
    }
    this.datasetConfigPrefix = properties.getProperty(HIVE_DATASET_CONFIG_PREFIX_KEY, DEFAULT_HIVE_DATASET_CONIFG_PREFIX);
    this.configClient = configClient;
    try {
      this.configStoreDatasetUriBuilder =
          properties.containsKey(CONFIG_STORE_DATASET_URI_BUILDER_CLASS) ? (Function<Table, String>) ConstructorUtils
              .invokeConstructor(Class.forName(properties.getProperty(CONFIG_STORE_DATASET_URI_BUILDER_CLASS)))
              : DEFAULT_CONFIG_STORE_DATASET_URI_BUILDER;
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException
        | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.jobConfig = ConfigUtils.propertiesToConfig(properties);
  }

  protected static HiveMetastoreClientPool createClientPool(Properties properties) throws IOException {
    return HiveMetastoreClientPool.get(properties,
        Optional.fromNullable(properties.getProperty(HIVE_METASTORE_URI_KEY)));
  }

  /**
   * Get all tables in db with given table pattern.
   */
  public Collection<DbAndTable> getTables() throws IOException {
    List<DbAndTable> tables = Lists.newArrayList();

    try (AutoReturnableObject<IMetaStoreClient> client = this.clientPool.getClient()) {
      Iterable<String> databases = Iterables.filter(client.get().getAllDatabases(), new Predicate<String>() {
        @Override
        public boolean apply(String db) {
          return HiveDatasetFinder.this.whitelistBlacklist.acceptDb(db);
        }
      });
      for (final String db : databases) {

        Iterable<String> tableNames = Iterables.filter(client.get().getAllTables(db), new Predicate<String>() {
          @Override
          public boolean apply(String table) {
            return HiveDatasetFinder.this.whitelistBlacklist.acceptTable(db, table);
          }
        });
        for (String tableName : tableNames) {
          tables.add(new DbAndTable(db, tableName));
        }
      }
    } catch (Exception exc) {
      throw new IOException(exc);
    }

    return tables;
  }

  @Data
  public static class DbAndTable {
    private final String db;
    private final String table;

    @Override
    public String toString() {
      return String.format("%s.%s", this.db, this.table);
    }
  }

  @Override
  public List<HiveDataset> findDatasets() throws IOException {
    return Lists.newArrayList(getDatasetsIterator());
  }

  @Override
  public Iterator<HiveDataset> getDatasetsIterator() throws IOException {

    return new AbstractIterator<HiveDataset>() {
      private Iterator<DbAndTable> tables = getTables().iterator();

      @Override
      protected HiveDataset computeNext() {
        while (this.tables.hasNext()) {
          DbAndTable dbAndTable = this.tables.next();

          try (AutoReturnableObject<IMetaStoreClient> client = HiveDatasetFinder.this.clientPool.getClient()) {
            Table table = client.get().getTable(dbAndTable.getDb(), dbAndTable.getTable());
            Config datasetConfig = getDatasetConfig(table);
            if (ConfigUtils.getBoolean(datasetConfig, HIVE_DATASET_IS_BLACKLISTED_KEY, DEFAULT_HIVE_DATASET_IS_BLACKLISTED_KEY)) {
              continue;
            }

            if (HiveDatasetFinder.this.eventSubmitter.isPresent()) {
              SlaEventSubmitter.builder().datasetUrn(dbAndTable.toString())
              .eventSubmitter(HiveDatasetFinder.this.eventSubmitter.get()).eventName(DATASET_FOUND).build().submit();
            }

            return createHiveDataset(table, datasetConfig);
          } catch (Throwable t) {
            log.error(String.format("Failed to create HiveDataset for table %s.%s", dbAndTable.getDb(), dbAndTable.getTable()), t);

            if (HiveDatasetFinder.this.eventSubmitter.isPresent()) {
              SlaEventSubmitter.builder().datasetUrn(dbAndTable.toString())
                  .eventSubmitter(HiveDatasetFinder.this.eventSubmitter.get()).eventName(DATASET_ERROR)
                  .additionalMetadata(FAILURE_CONTEXT, t.toString()).build().submit();
            }
          }
        }
        return endOfData();
      }
    };
  }


  /**
   * @deprecated Use {@link #createHiveDataset(Table, Config)} instead
   */
  @Deprecated
  protected HiveDataset createHiveDataset(Table table) throws IOException {
    return createHiveDataset(table, ConfigFactory.empty());
  }

  protected HiveDataset createHiveDataset(Table table, Config datasetConfig) throws IOException {
    return new HiveDataset(this.fs, this.clientPool, new org.apache.hadoop.hive.ql.metadata.Table(table), this.properties, datasetConfig);
  }

  @Override
  public Path commonDatasetRoot() {
    return new Path("/");
  }

  /**
   * Gets the {@link Config} for this <code>dbAndTable</code>.
   * Cases:
   * <ul>
   * <li>If {@link #configStoreUri} is available it gets the dataset config from the config store at this uri
   * <li>If {@link #configStoreUri} is not available it uses the job config as dataset config
   * <li>If {@link #datasetConfigPrefix} is specified, only configs with this prefix is returned
   * <li>If {@link #datasetConfigPrefix} is not specified, all configs are returned
   * </ul>
   * @param table of the dataset to get config
   * @return the {@link Config} for <code>dbAndTable</code>
   */
  private Config getDatasetConfig(Table table) throws ConfigStoreFactoryDoesNotExistsException,
      ConfigStoreCreationException, URISyntaxException {

    Config datasetConfig;
    Optional<Config> runtimeConfig = ConfigClientUtils.getOptionalRuntimeConfig(properties);
    // Config store enabled
    if (this.configStoreUri.isPresent()) {
      if (runtimeConfig.isPresent()) {
        datasetConfig = this.configClient.getConfig(
            this.configStoreUri.get() + Path.SEPARATOR + this.configStoreDatasetUriBuilder.apply(table),
            runtimeConfig);
      } else {
        datasetConfig = this.configClient.getConfig(
            this.configStoreUri.get() + Path.SEPARATOR + this.configStoreDatasetUriBuilder.apply(table));
      }

    // If config store is not enabled use job config
    } else {
      datasetConfig = this.jobConfig;
    }

    return StringUtils.isBlank(this.datasetConfigPrefix) ? datasetConfig : ConfigUtils.getConfig(datasetConfig,
        this.datasetConfigPrefix, ConfigFactory.empty());
  }

  private static final Function<Table, String> DEFAULT_CONFIG_STORE_DATASET_URI_BUILDER =
      new Function<Table, String>() {

        @Override
        public String apply(@Nonnull Table table) {
          return HiveConfigClientUtils.getDatasetUri(table);
        }
      };
}
