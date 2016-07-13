/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gobblin.data.management.conversion.hive.validation;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import azkaban.jobExecutor.AbstractJob;

import com.beust.jcommander.internal.Maps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
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
import gobblin.data.management.copy.hive.HiveUtils;
import gobblin.hive.util.HiveJdbcConnector;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.util.AutoReturnableObject;
import gobblin.util.ConfigUtils;
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
  private static final String DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS = "3";
  private static final String DEFAULT_HIVE_SOURCE_SKIP_RECENT_THAN_DAYS = "1";

  private final Properties props;
  private final MetricContext metricContext;
  private final EventSubmitter eventSubmitter;
  private final HiveUnitUpdateProvider updateProvider;
  private final ConvertibleHiveDatasetFinder datasetFinder;
  private final long maxLookBackTime;
  private final long skipRecentThanTime;
  private final Set<String> destFormats;
  private final HiveJdbcConnector hiveJdbcConnector;

  private Map<String, String> successfulConversions;
  private Map<String, String> failedConversions;

  public ValidationJob(String jobId, Properties props) throws IOException {
    super(jobId, log);

    Config config = ConfigFactory.parseProperties(props);
    this.props = props;
    this.metricContext = Instrumented.getMetricContext(ConfigUtils.configToState(config), ValidationJob.class);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, EventConstants.CONVERSION_NAMESPACE).build();
    this.updateProvider = UpdateProviderFactory.create(props);
    this.datasetFinder = new ConvertibleHiveDatasetFinder(getSourceFs(), props, this.eventSubmitter);

    int maxLookBackDays = Integer.parseInt(props.getProperty(HiveSource.HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS_KEY,
        DEFAULT_HIVE_SOURCE_MAXIMUM_LOOKBACK_DAYS));
    int skipRecentThanDays = Integer.parseInt(props.getProperty(HIVE_SOURCE_SKIP_RECENT_THAN_DAYS_KEY,
        DEFAULT_HIVE_SOURCE_SKIP_RECENT_THAN_DAYS));
    this.maxLookBackTime = new DateTime().minusDays(maxLookBackDays).getMillis();
    this.skipRecentThanTime = new DateTime().minusDays(skipRecentThanDays).getMillis();

    // value for DESTINATION_CONVERSION_FORMATS_KEY can be a TypeSafe list or a comma separated list of string
    this.destFormats = Sets.newHashSet(
        ConfigUtils.getStringList(config, ConvertibleHiveDataset.DESTINATION_CONVERSION_FORMATS_KEY));

    try {
      this.hiveJdbcConnector = HiveJdbcConnector.newConnectorWithProps(props);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    EventSubmitter.submit(Optional.of(this.eventSubmitter), EventConstants.VALIDATION_SETUP_EVENT);
  }

  @Override
  public void run()
      throws Exception {
    try {
      // Validation results
      this.successfulConversions = Maps.newHashMap();
      this.failedConversions = Maps.newHashMap();

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

      // Log validation results:
      // Validation results are consolidated into the successfulConversions and failedConversions
      // These are then converted into log lines in the Azkaban logs as done below
      // If the validation fails for any dataset, the job is failed to gain attention of operator
      for (Map.Entry<String, String> successfulConversion : this.successfulConversions.entrySet()) {
        log.info(String.format("Successful conversion: %s [%s]", successfulConversion.getKey(),
            successfulConversion.getValue()));
      }
      for (Map.Entry<String, String> failedConverion : this.failedConversions.entrySet()) {
        log.warn(String
            .format("Failed conversion: %s [%s]", failedConverion.getKey(), failedConverion.getValue()));
      }

      // Fail job if any conversion had failed to gain attention
      if (failedConversions.size() > 0) {
        throw new RuntimeException("Atleast one conversion failed. Please review report above");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /***
   * Validate a {@link Table} if it was updated recently by checking if its update time
   * lies between between maxLookBackTime and skipRecentThanTime window.
   * @param hiveDataset {@link ConvertibleHiveDataset} containing {@link Table} info.
   * @throws IOException Issue in validating {@link HiveDataset}
   */
  private void processNonPartitionedTable(ConvertibleHiveDataset hiveDataset)
      throws IOException {
    try {
      // Validate table
      long updateTime = this.updateProvider.getUpdateTime(hiveDataset.getTable());
      if (shouldValidate(updateTime, this.maxLookBackTime, this.skipRecentThanTime)) {
        log.debug(String.format("Validating table: %s", hiveDataset.getTable()));

        for (String format : this.destFormats) {
          Optional<ConvertibleHiveDataset.ConversionConfig> conversionConfigOptional =
              hiveDataset.getConversionConfigForFormat(format);
          if (conversionConfigOptional.isPresent()) {
            ConvertibleHiveDataset.ConversionConfig conversionConfig = conversionConfigOptional.get();

            // Generate validation queries
            List<String> validationQueries = HiveValidationQueryGenerator
                .generateValidationQueries(hiveDataset, Optional.<Partition>absent(), conversionConfig);

            // Execute validation queries
            log.info(String.format("Going to execute queries: %s for format: %s", validationQueries, format));
            List<ResultSet> resultSets = this.executeQueries(validationQueries);

            // Validate and populate report
            validateAndPopulateReport(hiveDataset.getTable().getCompleteName(), updateTime, resultSets);
          } else {
            log.info(String.format("No config found for format: %s So skipping table: %s for this format", format,
                hiveDataset.getTable().getCompleteName()));
          }
        }
      } else {
        log.info(String.format("Not validating table: %s as updateTime: %s is not in range of max look back: %s "
                + "and skip recent than: %s", hiveDataset.getTable().getCompleteName(), updateTime,
            this.maxLookBackTime, this.skipRecentThanTime));
      }
    } catch (UpdateNotFoundException e) {
      log.info(String.format("Not validating table: %s as update time was not found. %s", hiveDataset.getTable()
          .getCompleteName(), e.getMessage()));
    }
  }

  /***
   * Validate all {@link Partition}s for a {@link Table} if it was updated recently by checking if its update time
   * lies between between maxLookBackTime and skipRecentThanTime window.
   * @param hiveDataset {@link HiveDataset} containing {@link Table} and {@link Partition} info.
   * @param client {@link IMetaStoreClient} to query Hive.
   * @throws IOException Issue in validating {@link HiveDataset}
   */
  private void processPartitionedTable(ConvertibleHiveDataset hiveDataset, AutoReturnableObject<IMetaStoreClient> client)
      throws IOException {

    // Get partitions for the table
    List<Partition>
        sourcePartitions = HiveUtils.getPartitions(client.get(), hiveDataset.getTable(), Optional.<String>absent());

    // Validate each partition
    for (Partition sourcePartition : sourcePartitions) {
      try {
        long updateTime = this.updateProvider.getUpdateTime(sourcePartition);
        if (shouldValidate(updateTime, this.maxLookBackTime, this.skipRecentThanTime)) {
          log.info(String.format("Validating partition: %s", sourcePartition));

          for (String format : this.destFormats) {
            Optional<ConvertibleHiveDataset.ConversionConfig> conversionConfigOptional =
                hiveDataset.getConversionConfigForFormat(format);
            if (conversionConfigOptional.isPresent()) {
              ConvertibleHiveDataset.ConversionConfig conversionConfig = conversionConfigOptional.get();

              // Generate validation queries
              List<String> validationQueries = HiveValidationQueryGenerator
                  .generateValidationQueries(hiveDataset, Optional.of(sourcePartition), conversionConfig);

              // Execute validation queries
              log.info(String.format("Going to execute queries: %s for format: %s", validationQueries, format));
              List<ResultSet> resultSets = this.executeQueries(validationQueries);

              // Validate and populate report
              validateAndPopulateReport(sourcePartition.getCompleteName(), updateTime, resultSets);
            } else {
              log.info(String.format("No config found for format: %s So skipping partition: %s for this format", format,
                  sourcePartition.getCompleteName()));
            }
          }
        } else {
          log.info(String.format("Not validating partition: %s as updateTime: %s is not in range of max look back: %s "
                  + "and skip recent than: %s",
              sourcePartition.getCompleteName(), updateTime, this.maxLookBackTime, this.skipRecentThanTime));
        }
      } catch (UpdateNotFoundException e) {
        log.info(String.format("Not validating partition: %s as update time was not found. %s",
            sourcePartition.getCompleteName(), e.getMessage()));
      }
    }
  }

  /***
   * Execute Hive queries using {@link HiveJdbcConnector} and validate results.
   * @param queries Queries to execute.
   */
  private List<ResultSet> executeQueries(List<String> queries) {
    if (null == queries || queries.size() == 0) {
      log.warn("No queries specified to be executed");
      return Collections.emptyList();
    }
    try {
       return this.hiveJdbcConnector
          .executeStatementsWithResult(queries.toArray(new String[queries.size()]));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void validateAndPopulateReport(String datasetIdentifier, long conversionInstance, List<ResultSet> resultSets) {
    if (null == resultSets || resultSets.size() == 0) {
      this.successfulConversions.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
          "No conversion details found");
      new SlaEventSubmitter(this.eventSubmitter, EventConstants.VALIDATION_NOOP_SLA_EVENT, props)
          .submit();
      return;
    }
    try {
      long rowCountCached = -1;
      boolean isFirst = true;
      for (ResultSet resultSet : resultSets) {
        if (resultSet.next()) {
          long rowCount = resultSet.getLong(1);
          if (isFirst) {
            rowCountCached = rowCount;
            isFirst = false;
            continue;
          }
          if (rowCount != rowCountCached) {
            this.failedConversions.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
                "Row counts did not match across all conversions");
            new SlaEventSubmitter(this.eventSubmitter, EventConstants.VALIDATION_FAILED_SLA_EVENT, props)
                .submit();
            return;
          }
        }
      }
      this.successfulConversions.put(String.format("Dataset: %s Instance: %s", datasetIdentifier, conversionInstance),
          "Row counts matched across all conversions");
      new SlaEventSubmitter(this.eventSubmitter, EventConstants.VALIDATION_SUCCESSFUL_SLA_EVENT, props)
          .submit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /***
   * Determine if the {@link Table} or {@link Partition} should be validated by checking if its last update time
   * lies between maxLookBackTime and skipRecentThanTime window.
   * @param updateTime Update line in milis for the {@link Table} or partition.
   * @param maxLookBackTime Maximum look back time in millis.
   * @param skipRecentThanTime Skip recent than time in millis.
   * @return If {@link Table} or {@link Partition} should be validated.
   */
  @VisibleForTesting
  public static boolean shouldValidate(long updateTime, long maxLookBackTime, long skipRecentThanTime) {
    DateTime updateDateTime = new DateTime(updateTime);
    return updateDateTime.isAfter(maxLookBackTime)
        && updateDateTime.isBefore(skipRecentThanTime);
  }

  /***
   * Get source {@link FileSystem}
   * @return Source {@link FileSystem}
   * @throws IOException Issue in fetching {@link FileSystem}
   */
  private static FileSystem getSourceFs() throws IOException {
    return FileSystem.get(HadoopUtils.newConfiguration());
  }
}
