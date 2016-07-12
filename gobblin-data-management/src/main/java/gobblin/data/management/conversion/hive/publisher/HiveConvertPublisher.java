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
package gobblin.data.management.conversion.hive.publisher;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.configuration.WorkUnitState.WorkingState;
import gobblin.data.management.conversion.hive.AvroSchemaManager;
import gobblin.data.management.conversion.hive.events.EventConstants;
import gobblin.data.management.conversion.hive.query.HiveAvroORCQueryGenerator;
import gobblin.data.management.conversion.hive.watermarker.TableLevelWatermarker;
import gobblin.hive.util.HiveJdbcConnector;
import gobblin.instrumented.Instrumented;
import gobblin.metrics.MetricContext;
import gobblin.metrics.event.EventSubmitter;
import gobblin.metrics.event.sla.SlaEventSubmitter;
import gobblin.publisher.DataPublisher;
import gobblin.source.extractor.extract.LongWatermark;
import gobblin.util.HadoopUtils;


/**
 * A simple {@link DataPublisher} updates the watermark and working state
 */
public class HiveConvertPublisher extends DataPublisher {

  private final AvroSchemaManager avroSchemaManager;
  private final HiveJdbcConnector hiveJdbcConnector;
  private MetricContext metricContext;
  private EventSubmitter eventSubmitter;

  public HiveConvertPublisher(State state) throws IOException {
    super(state);
    this.avroSchemaManager = new AvroSchemaManager(FileSystem.get(HadoopUtils.newConfiguration()), state);
    this.metricContext = Instrumented.getMetricContext(state, HiveConvertPublisher.class);
    this.eventSubmitter = new EventSubmitter.Builder(this.metricContext, EventConstants.CONVERSION_NAMESPACE).build();
    try {
      this.hiveJdbcConnector = HiveJdbcConnector.newConnectorWithProps(state.getProperties());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void initialize() throws IOException {
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {

    String cleanupCommands = StringUtils.EMPTY;
    boolean isFirst = true;
    if (Iterables.tryFind(states, UNSUCCESSFUL_WORKUNIT).isPresent()) {
      for (WorkUnitState wus : states) {
        if (isFirst) {
          // Get cleanup staging table commands
          cleanupCommands = HiveAvroORCQueryGenerator.deserializeCleanupCommands(wus);
          isFirst = false;
        }
        wus.setWorkingState(WorkingState.FAILED);
        new SlaEventSubmitter(eventSubmitter, EventConstants.CONVERSION_FAILED_EVENT, wus.getProperties()).submit();
      }
    } else {
      String publishTableCommands = StringUtils.EMPTY;
      for (WorkUnitState wus : states) {
        if (isFirst) {
          // Get publish table commands
          publishTableCommands = HiveAvroORCQueryGenerator.deserializePublishTableCommands(wus);

          // Execute publish table commands
          executeQueries(publishTableCommands);

          // Get cleanup staging table commands
          cleanupCommands = HiveAvroORCQueryGenerator.deserializeCleanupCommands(wus);
          isFirst = false;
        }

        // Get publish partition commands if any
        String publishPartitionCommands = HiveAvroORCQueryGenerator.deserializePublishPartitionCommands(wus);

        // Execute publish partition commands if any
        executeQueries(publishPartitionCommands);

        wus.setWorkingState(WorkingState.COMMITTED);
        wus.setActualHighWatermark(TableLevelWatermarker.GSON.fromJson(wus.getWorkunit().getExpectedHighWatermark(),
            LongWatermark.class));

        new SlaEventSubmitter(eventSubmitter, EventConstants.CONVERSION_SUCCESSFUL_SLA_EVENT, wus.getProperties())
            .submit();
      }
    }
    // Execute cleanup staging table commands
    executeQueries(cleanupCommands);
  }

  private void executeQueries(String queries) {
    if (StringUtils.isBlank(queries)) {
      return;
    }
    try {
      this.hiveJdbcConnector.executeStatements(queries);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
  }

  @Override
  public void close() throws IOException {
    this.avroSchemaManager.cleanupTempSchemas();
  }

  private static final Predicate<WorkUnitState> UNSUCCESSFUL_WORKUNIT = new Predicate<WorkUnitState>() {

    @Override
    public boolean apply(WorkUnitState input) {
      return null == input || !WorkingState.SUCCESSFUL.equals(input.getWorkingState());
    }
  };
}
