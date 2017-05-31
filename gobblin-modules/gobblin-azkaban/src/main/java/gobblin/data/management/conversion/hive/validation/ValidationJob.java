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
package gobblin.data.management.conversion.hive.validation;

import gobblin.config.client.ConfigClient;
import gobblin.config.client.api.VersionStabilityPolicy;
import gobblin.configuration.ConfigurationKeys;
import gobblin.util.PathUtils;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import azkaban.jobExecutor.AbstractJob;

import com.google.common.base.Charsets;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDataset;
import gobblin.data.management.conversion.hive.dataset.ConvertibleHiveDatasetFinder;
import gobblin.data.management.conversion.hive.events.EventConstants;
import gobblin.data.management.conversion.hive.provider.HiveUnitUpdateProvider;
import gobblin.data.management.conversion.hive.provider.UpdateNotFoundException;
import gobblin.data.management.conversion.hive.provider.UpdateProviderFactory;
import gobblin.data.management.conversion.hive.query.HiveValidationQueryGenerator;
import gobblin.data.management.conversion.hive.source.HiveSource;
import gobblin.data.management.copy.hive.HiveDataset;
import gobblin.data.management.copy.hive.HiveDatasetFinder;
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.hive.HiveMetastoreClientPool;
import gobblin.hive.HiveSerDeWrapper;
import gobblin.util.HiveJdbcConnector;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.util.AutoReturnableObject;
import gobblin.util.ConfigUtils;
import gobblin.util.ExecutorsUtils;
import gobblin.util.HadoopUtils;


/**
 * Azkaban job that runs validation of conversion between two Hive tables
 *
 * @author Abhishek Tiwari
 */
public class ValidationJob extends AbstractJob {
  private static final Logger log = Logger.getLogger(ValidationJob.class);

  /***
   * Validation Job validates the table and / or partitions updated within a specific window.
   * This window is determined as follows:
   * Start ($start_time) : CURRENT_TIME - hive.source.maximum.lookbackDays
   * End   ($end_time)   : CURRENT_TIME - hive.source.skip.recentThanDays
   * ie. the resultant window for validation is: $start_time <= window <= $end_time
   */
  private static final String HIVE_SOURCE_SKIP_RECENT_THAN_DAYS_KEY = "hive.source.skip.recentThanDays";
  private static final String HIVE_DATASET_CONFIG_AVRO_PREFIX = "hive.conversion.avro";
  private static final String DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS = "3";
  private static final String DEFAULT_HIVE_SOURCE_SKIP_RECENT_THAN_DAYS = "1";

  private static final String MAX_THREAD_COUNT = "validation.maxThreadCount";
  private static final String DEFAULT_MAX_THREAD_COUNT = "50";
  private static final String VALIDATION_TYPE_KEY = "hive.validation.type";
  private static final String HIVE_VALIDATION_IGNORE_DATA_PATH_IDENTIFIER_KEY = "hive.validation.ignoreDataPathIdentifier";
  private static final String DEFAULT_HIVE_VALIDATION_IGNORE_DATA_PATH_IDENTIFIER = org.apache.commons.lang.StringUtils.EMPTY;
  private static final Splitter COMMA_BASED_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
  private static final Splitter EQUALITY_SPLITTER = Splitter.on("=").omitEmptyStrings().trimResults();
  private static final Splitter SLASH_SPLITTER = Splitter.on("/").omitEmptyStrings().trimResults();
  private static final String VALIDATION_FILE_FORMAT_KEY = "hive.validation.fileFormat";
  private static final String IS_NESTED_ORC = "hive.validation.isNestedORC";
  private static final String DEFAULT_IS_NESTED_ORC = "false";
  private static final String HIVE_SETTINGS = "hive.settings";
  private static final String DATEPARTITION = "datepartition";
  private static final String DATE_FORMAT = "yyyy-MM-dd-HH";
  public static final String GOBBLIN_CONFIG_TAGS_WHITELIST = "gobblin.config.tags.whitelist";

  private final ValidationType validationType;
  private List<String> ignoreDataPathIdentifierList;
  private final List<Throwable> throwables;
  private final Properties props;
  private final MetricContext metricContext;
  private final EventSubmitter eventSubmitter;
  private final HiveUnitUpdateProvider updateProvider;
  private final ConvertibleHiveDatasetFinder datasetFinder;
  private final long maxLookBackTime;
  private final long skipRecentThanTime;
  private final HiveMetastoreClientPool pool;
  private final FileSystem fs;
  private final ExecutorService exec;
  private final List<Future<Void>> futures;
  private final Boolean isNestedORC;
  private final List<String> hiveSettings;
  protected Optional<String> configStoreUri;
  private static final short maxParts = 1000;

  private Map<String, String> successfulConversions;
  private Map<String, String> failedConversions;
  private Map<String, String> warnConversions;
  private Map<String, String> dataValidationFailed;
  private Map<String, String> dataValidationSuccessful;

  public ValidationJob(String jobId, Properties props) throws IOException {
    super(jobId, log);

    // Set the conversion config prefix for Avro to ORC
    props.setProperty(HiveDatasetFinder.HIVE_DATASET_CONFIG_PREFIX_KEY, HIVE_DATASET_CONFIG_AVRO_PREFIX);

    Config config = ConfigFactory.parseProperties(props);
    this.props = props;
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), ValidationJob.class);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, EventConstants.CONVERSION_NAMESPACE).build();
    this.updateProvider = UpdateProviderFactory.create(props);
    this.datasetFinder = new ConvertibleHiveDatasetFinder(getSourceFs(), props, this.eventSubmitter);
    this.fs = FileSystem.get(new Configuration());

    int maxLookBackDays = Integer.parseInt(props.getProperty(HiveSource.HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY, DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS));
    int skipRecentThanDays = Integer.parseInt(props.getProperty(HIVE_SOURCE_SKIP_RECENT_THAN_DAYS_KEY, DEFAULT_HIVE_SOURCE_SKIP_RECENT_THAN_DAYS));
    this.maxLookBackTime = new DateTime().minusDays(maxLookBackDays).getMillis();
    this.skipRecentThanTime = new DateTime().minusDays(skipRecentThanDays).getMillis();

    int maxThreadCount = Integer.parseInt(props.getProperty(MAX_THREAD_COUNT, DEFAULT_MAX_THREAD_COUNT));
    this.exec =
        Executors.newFixedThreadPool(maxThreadCount,
            ExecutorsUtils.newThreadFactory(Optional.of(LoggerFactory.getLogger(ValidationJob.class)), Optional.of("getValidationOutputFromHive")));
    this.futures = Lists.newArrayList();
    EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.VALIDATION_SETUP_EVENT);

    this.pool = HiveMetastoreClientPool.get(props, Optional.fromNullable(props.getProperty(HiveDatasetFinder.HIVE_METASTORE_URI_KEY)));
    Preconditions.checkArgument(props.containsKey(VALIDATION_TYPE_KEY), "Missing property " + VALIDATION_TYPE_KEY);
    this.validationType = ValidationType.valueOf(props.getProperty(VALIDATION_TYPE_KEY));
    this.ignoreDataPathIdentifierList = COMMA_BASED_SPLITTER.splitToList(props
        .getProperty(HIVE_VALIDATION_IGNORE_DATA_PATH_IDENTIFIER_KEY,
            DEFAULT_HIVE_VALIDATION_IGNORE_DATA_PATH_IDENTIFIER));
    this.throwables = new ArrayList<>();
    this.isNestedORC = Boolean.parseBoolean(props.getProperty(IS_NESTED_ORC, DEFAULT_IS_NESTED_ORC));
    this.hiveSettings = Splitter.on(";").trimResults().omitEmptyStrings()
        .splitToList(props.getProperty(HIVE_SETTINGS, StringUtils.EMPTY));
  }

  @Override
  public void run() throws Exception {
    if (this.validationType == ValidationType.COUNT_VALIDATION) {
      runCountValidation();
    } else if (this.validationType == ValidationType.FILE_FORMAT_VALIDATION) {
      runFileFormatValidation();
    }
  }

  /**
   * Validates that partitions are in a given format
   * Partitions to be processed are picked up from the config store which are tagged.
   * Tag can be passed through key GOBBLIN_CONFIG_TAGS_WHITELIST
   * Datasets tagged by the above key will be picked up.
   * PathName will be treated as tableName and ParentPathName will be treated as dbName
   *
   * For example if the dataset uri picked up by is /data/hive/myDb/myTable
   * Then myTable is tableName and myDb is dbName
   */
  private void runFileFormatValidation() throws IOException {
    Preconditions.checkArgument(this.props.containsKey(VALIDATION_FILE_FORMAT_KEY));

    this.configStoreUri =
        StringUtils.isNotBlank(this.props.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI)) ? Optional.of(
            this.props.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_URI)) : Optional.<String>absent();
    if (!Boolean.valueOf(this.props.getProperty(ConfigurationKeys.CONFIG_MANAGEMENT_STORE_ENABLED,
        ConfigurationKeys.DEFAULT_CONFIG_MANAGEMENT_STORE_ENABLED))) {
      this.configStoreUri = Optional.<String>absent();
    }
    List<Partition> partitions = new ArrayList<>();
    if (this.configStoreUri.isPresent()) {
      Preconditions.checkArgument(this.props.containsKey(GOBBLIN_CONFIG_TAGS_WHITELIST),
          "Missing required property " + GOBBLIN_CONFIG_TAGS_WHITELIST);
      String tag = this.props.getProperty(GOBBLIN_CONFIG_TAGS_WHITELIST);
      ConfigClient configClient = ConfigClient.createConfigClient(VersionStabilityPolicy.WEAK_LOCAL_STABILITY);
      Path tagUri = PathUtils.mergePaths(new Path(this.configStoreUri.get()), new Path(tag));
      try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
        Collection<URI> importedBy = configClient.getImportedBy(new URI(tagUri.toString()), true);
        for (URI uri : importedBy) {
          String dbName = new Path(uri).getParent().getName();
          Table table = new Table(client.get().getTable(dbName, new Path(uri).getName()));
          for (org.apache.hadoop.hive.metastore.api.Partition partition : client.get()
              .listPartitions(dbName, table.getTableName(), maxParts)) {
            partitions.add(new Partition(table, partition));
          }
        }
      } catch (Exception e) {
        this.throwables.add(e);
      }
    }

    for (Partition partition : partitions) {
      if (!shouldValidate(partition)) {
        continue;
      }
      String fileFormat = this.props.getProperty(VALIDATION_FILE_FORMAT_KEY);
      Optional<HiveSerDeWrapper.BuiltInHiveSerDe> hiveSerDe =
          Enums.getIfPresent(HiveSerDeWrapper.BuiltInHiveSerDe.class, fileFormat.toUpperCase());
      if (!hiveSerDe.isPresent()) {
        throwables.add(new Throwable("Partition SerDe is either not supported or absent"));
        continue;
      }

      String serdeLib = partition.getTPartition().getSd().getSerdeInfo().getSerializationLib();
      if (!hiveSerDe.get().toString().equalsIgnoreCase(serdeLib)) {
        throwables.add(new Throwable("Partition " + partition.getCompleteName() + " SerDe " + serdeLib
            + " doesn't match with the required SerDe " + hiveSerDe.get().toString()));
      }
    }
    if (!this.throwables.isEmpty()) {
      for (Throwable e : this.throwables) {
        log.error("Failed to validate due to " + e);
      }
      throw new RuntimeException("Validation Job Failed");
    }
  }

  private void runCountValidation()
      throws InterruptedException {
    try {
      // Validation results
      this.successfulConversions = Maps.newConcurrentMap();
      this.failedConversions = Maps.newConcurrentMap();
      this.warnConversions = Maps.newConcurrentMap();
      this.dataValidationFailed = Maps.newConcurrentMap();
      this.dataValidationSuccessful = Maps.newConcurrentMap();

      // Find datasets to validate
      Iterator<HiveDataset> iterator = this.datasetFinder.getDatasetsIterator();
      EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.VALIDATION_FIND_HIVE_TABLES_EVENT);

      while (iterator.hasNext()) {
        ConvertibleHiveDataset hiveDataset = (ConvertibleHiveDataset) iterator.next();
        try (AutoReturnableObject<IMetaStoreClient> client = hiveDataset.getClientPool().getClient()) {

          // Validate dataset
          log.info(String.format("Validating dataset: %s", hiveDataset));
          if (HiveUtils.isPartitioned(hiveDataset.getTable())) {
            processPartitionedTable(hiveDataset, client);
          } else {
            processNonPartitionedTable(hiveDataset);
          }
        }
      }

      // Wait for all validation queries to finish
      log.info(String.format("Waiting for %d futures to complete", this.futures.size()));

      this.exec.shutdown();
      this.exec.awaitTermination(4, TimeUnit.HOURS);

      boolean oneFutureFailure = false;
      // Check if there were any exceptions
      for (Future<Void> future : this.futures) {
        try {
          future.get();
        } catch (Throwable t) {
          log.error("getValidationOutputFromHive failed", t);
          oneFutureFailure = true;
        }
      }

      // Log validation results:
      // Validation results are consolidated into the successfulConversions and failedConversions
      // These are then converted into log lines in the Azkaban logs as done below
      for (Map.Entry<String, String> successfulConversion : this.successfulConversions.entrySet()) {
        log.info(String.format("Successful conversion: %s [%s]", successfulConversion.getKey(), successfulConversion.getValue()));
      }
      for (Map.Entry<String, String> successfulConversion : this.warnConversions.entrySet()) {
        log.warn(String.format("No conversion found for: %s [%s]", successfulConversion.getKey(), successfulConversion.getValue()));
      }
      for (Map.Entry<String, String> failedConverion : this.failedConversions.entrySet()) {
        log.error(String.format("Failed conversion: %s [%s]", failedConverion.getKey(), failedConverion.getValue()));
      }

      for (Map.Entry<String, String> success : this.dataValidationSuccessful.entrySet()) {
        log.info(String.format("Data validation successful: %s [%s]", success.getKey(), success.getValue()));
      }

      for (Map.Entry<String, String> failed : this.dataValidationFailed.entrySet()) {
        log.error(String.format("Data validation failed: %s [%s]", failed.getKey(), failed.getValue()));
      }

      if (!this.failedConversions.isEmpty() || !this.dataValidationFailed.isEmpty()) {
        throw new RuntimeException(String.format("Validation failed for %s conversions. See previous logs for exact validation failures",
            failedConversions.size()));
      }
      if (oneFutureFailure) {
        throw new RuntimeException("At least one hive ddl failed. Check previous logs");
      }

    } catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  /***
   * Validate a {@link Table} if it was updated recently by checking if its update time
   * lies between between maxLookBackTime and skipRecentThanTime window.
   * @param hiveDataset {@link ConvertibleHiveDataset} containing {@link Table} info.
   * @throws IOException Issue in validating {@link HiveDataset}
   */
  private void processNonPartitionedTable(final ConvertibleHiveDataset hiveDataset) throws IOException {
    try {
      // Validate table
      final long updateTime = this.updateProvider.getUpdateTime(hiveDataset.getTable());

      log.info(String.format("Validating table: %s", hiveDataset.getTable()));

      for (final String format : hiveDataset.getDestFormats()) {
        Optional<ConvertibleHiveDataset.ConversionConfig> conversionConfigOptional = hiveDataset.getConversionConfigForFormat(format);
        if (conversionConfigOptional.isPresent()) {
          ConvertibleHiveDataset.ConversionConfig conversionConfig = conversionConfigOptional.get();
          String orcTableName = conversionConfig.getDestinationTableName();
          String orcTableDatabase = conversionConfig.getDestinationDbName();
          Pair<Optional<org.apache.hadoop.hive.metastore.api.Table>, Optional<List<Partition>>> destinationMeta =
              getDestinationTableMeta(orcTableDatabase, orcTableName, this.props);

          // Generate validation queries
          final List<String> validationQueries =
              HiveValidationQueryGenerator.generateCountValidationQueries(hiveDataset, Optional.<Partition> absent(), conversionConfig);
          final List<String> dataValidationQueries =
              Lists.newArrayList(HiveValidationQueryGenerator.generateDataValidationQuery(hiveDataset.getTable().getTableName(), hiveDataset.getTable()
                  .getDbName(), destinationMeta.getKey().get(), Optional.<Partition> absent(), this.isNestedORC));

          this.futures.add(this.exec.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {

              // Execute validation queries
              log.debug(String.format("Going to execute queries: %s for format: %s", validationQueries, format));
              List<Long> rowCounts = ValidationJob.this.getValidationOutputFromHive(validationQueries);
              log.debug(String.format("Going to execute queries: %s for format: %s", dataValidationQueries, format));
              List<Long> rowDataValidatedCount = ValidationJob.this.getValidationOutputFromHive(dataValidationQueries);
              // Validate and populate report
              validateAndPopulateReport(hiveDataset.getTable().getCompleteName(), updateTime, rowCounts, rowDataValidatedCount.get(0));

              return null;
            }
          }));
        } else {
          log.warn(String.format("No config found for format: %s So skipping table: %s for this format", format, hiveDataset.getTable().getCompleteName()));
        }
      }
    } catch (UncheckedExecutionException e) {
      log.warn(String.format("Not validating table: %s %s", hiveDataset.getTable().getCompleteName(), e.getMessage()));
    } catch (UpdateNotFoundException e) {
      log.warn(String
          .format("Not validating table: %s as update time was not found. %s", hiveDataset.getTable().getCompleteName(),
              e.getMessage()));
    }
  }

  /***
   * Validate all {@link Partition}s for a {@link Table} if it was updated recently by checking if its update time
   * lies between between maxLookBackTime and skipRecentThanTime window.
   * @param hiveDataset {@link HiveDataset} containing {@link Table} and {@link Partition} info.
   * @param client {@link IMetaStoreClient} to query Hive.
   * @throws IOException Issue in validating {@link HiveDataset}
   */
  private void processPartitionedTable(ConvertibleHiveDataset hiveDataset, AutoReturnableObject<IMetaStoreClient> client) throws IOException {

    // Get partitions for the table
    List<Partition> sourcePartitions = HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), Optional.<String> absent());

    for (final String format : hiveDataset.getDestFormats()) {
      Optional<ConvertibleHiveDataset.ConversionConfig> conversionConfigOptional = hiveDataset.getConversionConfigForFormat(format);

      if (conversionConfigOptional.isPresent()) {

        // Get conversion config
        ConvertibleHiveDataset.ConversionConfig conversionConfig = conversionConfigOptional.get();
        String orcTableName = conversionConfig.getDestinationTableName();
        String orcTableDatabase = conversionConfig.getDestinationDbName();
        Pair<Optional<org.apache.hadoop.hive.metastore.api.Table>, Optional<List<Partition>>> destinationMeta =
            getDestinationTableMeta(orcTableDatabase, orcTableName, this.props);

        // Validate each partition
        for (final Partition sourcePartition : sourcePartitions) {
          try {
            final long updateTime = this.updateProvider.getUpdateTime(sourcePartition);
            if (shouldValidate(sourcePartition)) {
              log.info(String.format("Validating partition: %s", sourcePartition.getCompleteName()));

              // Generate validation queries
              final List<String> countValidationQueries =
                  HiveValidationQueryGenerator.generateCountValidationQueries(hiveDataset, Optional.of(sourcePartition), conversionConfig);
              final List<String> dataValidationQueries =
                  Lists.newArrayList(HiveValidationQueryGenerator.generateDataValidationQuery(hiveDataset.getTable().getTableName(), hiveDataset.getTable()
                      .getDbName(), destinationMeta.getKey().get(), Optional.of(sourcePartition), this.isNestedORC));

              this.futures.add(this.exec.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {

                  // Execute validation queries
                  log.debug(String.format("Going to execute count validation queries queries: %s for format: %s "
                      + "and partition %s", countValidationQueries, format, sourcePartition.getCompleteName()));
                  List<Long> rowCounts = ValidationJob.this.getValidationOutputFromHive(countValidationQueries);
                  log.debug(String.format("Going to execute data validation queries: %s for format: %s and partition %s",
                      dataValidationQueries, format, sourcePartition.getCompleteName()));
                  List<Long> rowDataValidatedCount = ValidationJob.this.getValidationOutputFromHive(dataValidationQueries);

                  // Validate and populate report
                  validateAndPopulateReport(sourcePartition.getCompleteName(), updateTime, rowCounts, rowDataValidatedCount.get(0));

                  return null;
                }
              }));

            } else {
              log.debug(String.format("Not validating partition: %s as updateTime: %s is not in range of max look back: %s " + "and skip recent than: %s",
                  sourcePartition.getCompleteName(), updateTime, this.maxLookBackTime, this.skipRecentThanTime));
            }
          } catch (UncheckedExecutionException e) {
            log.warn(
                String.format("Not validating partition: %s %s", sourcePartition.getCompleteName(), e.getMessage()));
          } catch (UpdateNotFoundException e) {
            log.warn(String.format("Not validating partition: %s as update time was not found. %s",
                sourcePartition.getCompleteName(), e.getMessage()));
          }
        }
      } else {
        log.info(String.format("No conversion config found for format %s. Ignoring data validation", format));
      }
    }
  }

  /***
   * Execute Hive queries using {@link HiveJdbcConnector} and validate results.
   * @param queries Queries to execute.
   */
  @SuppressWarnings("unused")
  private List<Long> getValidationOutputFromHiveJdbc(List<String> queries) throws IOException {
    if (null == queries || queries.size() == 0) {
      log.warn("No queries specified to be executed");
      return Collections.emptyList();
    }
    Statement statement = null;
    List<Long> rowCounts = Lists.newArrayList();
    Closer closer = Closer.create();

    try {
      HiveJdbcConnector hiveJdbcConnector = HiveJdbcConnector.newConnectorWithProps(props);
      statement = hiveJdbcConnector.getConnection().createStatement();

      for (String query : queries) {
        log.info("Executing query: " + query);
        boolean result = statement.execute(query);
        if (result) {
          ResultSet resultSet = statement.getResultSet();
          if (resultSet.next()) {
            rowCounts.add(resultSet.getLong(1));
          }
        } else {
          log.warn("Query output for: " + query + " : " + result);
        }
      }

    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        closer.close();
      } catch (Exception e) {
        log.warn("Could not close HiveJdbcConnector", e);
      }
      if (null != statement) {
        try {
          statement.close();
        } catch (SQLException e) {
          log.warn("Could not close Hive statement", e);
        }
      }
    }

    return rowCounts;
  }

  /***
   * Execute Hive queries using {@link HiveJdbcConnector} and validate results.
   * @param queries Queries to execute.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", justification = "Temporary fix")
  private List<Long> getValidationOutputFromHive(List<String> queries) throws IOException {

    if (null == queries || queries.size() == 0) {
      log.warn("No queries specified to be executed");
      return Collections.emptyList();
    }

    List<Long> rowCounts = Lists.newArrayList();
    Closer closer = Closer.create();

    try {
      HiveJdbcConnector hiveJdbcConnector = closer.register(HiveJdbcConnector.newConnectorWithProps(props));
      for (String query : queries) {
        String hiveOutput = "hiveConversionValidationOutput_" + UUID.randomUUID().toString();
        Path hiveTempDir = new Path("/tmp" + Path.SEPARATOR + hiveOutput);
        query = "INSERT OVERWRITE DIRECTORY '" + hiveTempDir + "' " + query;
        log.info("Executing query: " + query);
        try {
          if (this.hiveSettings.size() > 0) {
            hiveJdbcConnector.executeStatements(this.hiveSettings.toArray(new String[this.hiveSettings.size()]));
          }
          hiveJdbcConnector.executeStatements("SET hive.exec.compress.output=false","SET hive.auto.convert.join=false", query);
          FileStatus[] fileStatusList = this.fs.listStatus(hiveTempDir);
          List<FileStatus> files = new ArrayList<>();
          for (FileStatus fileStatus : fileStatusList) {
            if (fileStatus.isFile()) {
              files.add(fileStatus);
            }
          }
          if (files.size() > 1) {
            log.warn("Found more than one output file. Should have been one.");
          } else if (files.size() == 0) {
            log.warn("Found no output file. Should have been one.");
          } else {
            String theString = IOUtils.toString(new InputStreamReader(this.fs.open(files.get(0).getPath()), Charsets.UTF_8));
            log.info("Found row count: " + theString.trim());
            if (StringUtils.isBlank(theString.trim())) {
              rowCounts.add(0l);
            } else {
              try {
                rowCounts.add(Long.parseLong(theString.trim()));
              } catch (NumberFormatException e) {
                throw new RuntimeException("Could not parse Hive output: " + theString.trim(), e);
              }
            }
          }
        } finally {
          if (this.fs.exists(hiveTempDir)) {
            log.debug("Deleting temp dir: " + hiveTempDir);
            this.fs.delete(hiveTempDir, true);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        closer.close();
      } catch (Exception e) {
        log.warn("Could not close HiveJdbcConnector", e);
      }
    }

    return rowCounts;
  }

  private void validateAndPopulateReport(String datasetIdentifier, long conversionInstance, List<Long> rowCounts, Long rowDataValidatedCount) {
    if (null == rowCounts || rowCounts.size() == 0) {
      this.warnConversions.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance), "No conversion details found");
      this.eventSubmitter.submit(EventConstants.VALIDATION_NOOP_EVENT, ImmutableMap.of("datasetUrn", datasetIdentifier));
      return;
    }
    long rowCountCached = -1;
    boolean isFirst = true;
    for (Long rowCount : rowCounts) {
      // First is always source partition / table (refer HiveValidationQueryGenerator)
      if (isFirst) {
        rowCountCached = rowCount;
        isFirst = false;
        continue;
      }

      // Row count validation
      if (rowCount != rowCountCached) {
        if (rowCount == 0) {
          this.warnConversions.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
              "Row counts found 0, may be the conversion is delayed.");
          this.eventSubmitter.submit(EventConstants.VALIDATION_NOOP_EVENT, ImmutableMap.of("datasetUrn", datasetIdentifier));
        } else {
          this.failedConversions.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
              String.format("Row counts did not match across all conversions. Row count expected: %d, Row count got: %d", rowCountCached, rowCount));
          this.eventSubmitter.submit(EventConstants.VALIDATION_FAILED_EVENT, ImmutableMap.of("datasetUrn", datasetIdentifier));
          return;
        }
      } else {
        this.successfulConversions.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
            String.format("Row counts matched across all conversions. Row count expected: %d, Row count got: %d", rowCountCached, rowCount));
        this.eventSubmitter.submit(EventConstants.VALIDATION_SUCCESSFUL_EVENT, ImmutableMap.of("datasetUrn", datasetIdentifier));
      }
    }

    // Data count validation
    if (rowCountCached == rowDataValidatedCount) {
      this.dataValidationSuccessful.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
          "Common rows matched expected value. Expected: " + rowCountCached + " Found: " + rowDataValidatedCount);
    } else {
      this.dataValidationFailed.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
          "Common rows did not match expected value. Expected: " + rowCountCached + " Found: " + rowDataValidatedCount);
    }
  }

  /***
   * Get source {@link FileSystem}
   * @return Source {@link FileSystem}
   * @throws IOException Issue in fetching {@link FileSystem}
   */
  private static FileSystem getSourceFs() throws IOException {
    return FileSystem.get(HadoopUtils.newConfiguration());
  }

  /**
   * Determine if the {@link Table} or {@link Partition} should be validated by checking if its create time
   * lies between maxLookBackTime and skipRecentThanTime window.
   */
  private boolean shouldValidate(Partition partition) {
    for (String pathToken : this.ignoreDataPathIdentifierList) {
      if (partition.getDataLocation().toString().toLowerCase().contains(pathToken.toLowerCase())) {
        log.info("Skipping partition " + partition.getCompleteName() + " containing invalid token " + pathToken
            .toLowerCase());
        return false;
      }
    }

    try {
      long createTime = getPartitionCreateTime(partition.getName());
      boolean withinTimeWindow = new DateTime(createTime).isAfter(this.maxLookBackTime) && new DateTime(createTime)
          .isBefore(this.skipRecentThanTime);
      if (!withinTimeWindow) {
        log.info("Skipping partition " + partition.getCompleteName() + " as create time " + new DateTime(createTime)
            .toString() + " is not within validation time window ");
      } else {
        log.info("Validating partition " + partition.getCompleteName());
        return withinTimeWindow;
      }
    } catch (ParseException e) {
      Throwables.propagate(e);
    }
    return false;
  }

  public static Long getPartitionCreateTime(String partitionName)
      throws ParseException {
    String dateString = null;
    for (String st : SLASH_SPLITTER.splitToList(partitionName)) {
      if (st.startsWith(DATEPARTITION)) {
        dateString = EQUALITY_SPLITTER.splitToList(st).get(1);
      }
    }
    Preconditions.checkNotNull(dateString, "Unable to get partition date");
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    return dateFormat.parse(dateString).getTime();
  }

  private Pair<Optional<org.apache.hadoop.hive.metastore.api.Table>, Optional<List<Partition>>> getDestinationTableMeta(String dbName, String tableName,
      Properties props) {

    Optional<org.apache.hadoop.hive.metastore.api.Table> table = Optional.absent();
    Optional<List<Partition>> partitions = Optional.absent();

    try {
      try (AutoReturnableObject<IMetaStoreClient> client = pool.getClient()) {
        table = Optional.of(client.get().getTable(dbName, tableName));
        if (table.isPresent()) {
          org.apache.hadoop.hive.ql.metadata.Table qlTable = new org.apache.hadoop.hive.ql.metadata.Table(table.get());
          if (HiveUtils.isPartitioned(qlTable)) {
            partitions = Optional.of(HiveUtils.getPartitions(client.get(), qlTable, Optional.<String> absent()));
          }
        }
      }
    } catch (NoSuchObjectException e) {
      return ImmutablePair.of(table, partitions);
    } catch (IOException | TException e) {
      throw new RuntimeException("Could not fetch destination table metadata", e);
    }

    return ImmutablePair.of(table, partitions);
  }
}

enum ValidationType {
  COUNT_VALIDATION, FILE_FORMAT_VALIDATION;

  ValidationType() {
  }
}
