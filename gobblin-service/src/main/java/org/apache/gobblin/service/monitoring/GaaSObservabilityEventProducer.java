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

package org.apache.gobblin.service.monitoring;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.codahale.metrics.MetricRegistry;
import com.typesafe.config.Config;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.ContextAwareMeter;
import org.apache.gobblin.metrics.GaaSObservabilityEventExperimental;
import org.apache.gobblin.metrics.Issue;
import org.apache.gobblin.metrics.IssueSeverity;
import org.apache.gobblin.metrics.JobStatus;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.ServiceMetricNames;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.runtime.troubleshooter.MultiContextIssueRepository;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterException;
import org.apache.gobblin.runtime.troubleshooter.TroubleshooterUtils;
import org.apache.gobblin.util.ConfigUtils;
import org.apache.gobblin.util.reflection.GobblinConstructorUtils;



/**
 * A class running along with data ingestion pipeline for emitting GobblinMCE (Gobblin Metadata Change Event
 * that includes the information of the file metadata change, i.e., add or delete file, and the column min/max value of the added file.
 * GMCE will be consumed by another metadata ingestion pipeline to register/de-register hive/iceberg metadata)
 *
 * This is an abstract class, we need a sub system like Kakfa, which support at least once delivery, to emit the event
 */
@Slf4j
public abstract class GaaSObservabilityEventProducer implements Closeable {
  public static final String GAAS_OBSERVABILITY_EVENT_PRODUCER_PREFIX = "GaaSObservabilityEventProducer.";
  public static final String GAAS_OBSERVABILITY_EVENT_ENABLED = GAAS_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "enabled";
  public static final String GAAS_OBSERVABILITY_EVENT_PRODUCER_CLASS = GAAS_OBSERVABILITY_EVENT_PRODUCER_PREFIX + "class.name";
  public static final String ISSUE_READ_ERROR_COUNT =  "GaaSObservability.producer.getIssuesFailedCount";

  protected MetricContext metricContext;
  protected State state;
  protected MultiContextIssueRepository issueRepository;
  ContextAwareMeter getIssuesFailedMeter;

  public GaaSObservabilityEventProducer(State state, MultiContextIssueRepository issueRepository) {
    this.metricContext = Instrumented.getMetricContext(state, getClass());
    getIssuesFailedMeter = this.metricContext.contextAwareMeter(MetricRegistry.name(ServiceMetricNames.GOBBLIN_SERVICE_PREFIX,
        ISSUE_READ_ERROR_COUNT));
    this.state = state;
    this.issueRepository = issueRepository;
  }

  public void emitObservabilityEvent(State jobStatus) {
    GaaSObservabilityEventExperimental event = createGaaSObservabilityEvent(jobStatus);
    sendUnderlyingEvent(event);
  }

  abstract protected void sendUnderlyingEvent(GaaSObservabilityEventExperimental event);

  private GaaSObservabilityEventExperimental createGaaSObservabilityEvent(State jobStatus) {
    Long jobStartTime = jobStatus.contains(TimingEvent.JOB_START_TIME) ? jobStatus.getPropAsLong(TimingEvent.JOB_START_TIME) : null;
    Long jobEndTime = jobStatus.contains(TimingEvent.JOB_END_TIME) ? jobStatus.getPropAsLong(TimingEvent.JOB_START_TIME) : null;
    GaaSObservabilityEventExperimental.Builder builder = GaaSObservabilityEventExperimental.newBuilder();
    List<Issue> issueList = null;
    try {
      issueList = issueRepository.getAll(TroubleshooterUtils.getContextIdForJob(jobStatus.getProperties())).stream().map(
              issue -> new org.apache.gobblin.metrics.Issue(issue.getTime().toEpochSecond(),
                  IssueSeverity.valueOf(issue.getSeverity().toString()), issue.getCode(), issue.getSummary(), issue.getDetails(), issue.getProperties())).collect(Collectors.toList());
    } catch (TroubleshooterException e) {
      log.error("Could not fetch issues while creating GaaSObservabilityEvent due to ", e);
      getIssuesFailedMeter.mark();
    }
      builder.setTimestamp(System.currentTimeMillis())
          .setFlowName(jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_NAME_FIELD))
          .setFlowGroup(jobStatus.getProp(TimingEvent.FlowEventConstants.FLOW_GROUP_FIELD))
          .setFlowExecutionId(jobStatus.getPropAsLong(TimingEvent.FlowEventConstants.FLOW_EXECUTION_ID_FIELD))
          .setJobName(jobStatus.getProp(TimingEvent.FlowEventConstants.JOB_NAME_FIELD))
          .setExecutorUrl(jobStatus.getProp(TimingEvent.METADATA_MESSAGE))
          .setJobStartTime(jobStartTime)
          .setJobEndTime(jobEndTime)
          .setJobStatus(JobStatus.valueOf(jobStatus.getProp(JobStatusRetriever.EVENT_NAME_FIELD)))
          .setIssues(issueList)
          // TODO: Populate the below fields in a separate PR
          .setExecutionUserUrn(null)
          .setExecutorId(null)
          .setLastFlowModificationTime(0)
          .setFlowGraphEdgeId(null)
          .setJobOrchestratedTime(null);
    return builder.build();
  }

  public static GaaSObservabilityEventProducer getEventProducer(Config config, MultiContextIssueRepository issueRepository) {
    return GobblinConstructorUtils.invokeConstructor(GaaSObservabilityEventProducer.class,
        config.getString(GAAS_OBSERVABILITY_EVENT_PRODUCER_CLASS), ConfigUtils.configToState(config), issueRepository);
  }

  @Override
  public void close() throws IOException {
    //producer close will handle by the cache
    this.metricContext.close();
  }
}
