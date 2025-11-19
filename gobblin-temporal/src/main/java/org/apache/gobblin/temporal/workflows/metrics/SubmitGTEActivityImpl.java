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

package org.apache.gobblin.temporal.workflows.metrics;

import java.util.Map;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.cluster.ContainerMetrics;
import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;
import org.apache.gobblin.metrics.kafka.KafkaAvroEventKeyValueReporter;
import org.apache.gobblin.temporal.GobblinTemporalConfigurationKeys;
import org.apache.gobblin.temporal.cluster.WorkerConfig;
import org.apache.gobblin.util.ConfigUtils;


@Slf4j
public class SubmitGTEActivityImpl implements SubmitGTEActivity {

    @Override
    public void submitGTE(GobblinEventBuilder eventBuilder, EventSubmitterContext eventSubmitterContext) {
        log.info("Submitting GTE - {}", summarizeEventMetadataForLogging(eventBuilder));
        eventSubmitterContext.create().submit(eventBuilder);
        Config config = WorkerConfig.of(this).orElse(ConfigFactory.load());
        if (config.hasPath(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_CONTAINER_METRICS_TASK_RUNNER_ID) &&
            config.hasPath(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_CONTAINER_METRICS_APPLICATION_NAME)) {
            String containerMetricsApplicationName = config.getString(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_CONTAINER_METRICS_APPLICATION_NAME);
            String containerMetricsTaskRunnerId = config.getString(GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_CONTAINER_METRICS_TASK_RUNNER_ID);
            ContainerMetrics containerMetrics = ContainerMetrics.get(ConfigUtils.configToState(config), containerMetricsApplicationName, containerMetricsTaskRunnerId);
            log.info("Fetched container metrics instance {} for taskRunnerId: {} and applicationName: {}", containerMetrics.toString(),
                containerMetricsTaskRunnerId, containerMetricsApplicationName);
            Optional<KafkaAvroEventKeyValueReporter> kafkaReporterOptional = containerMetrics.getScheduledReporter(KafkaAvroEventKeyValueReporter.class);
            if (kafkaReporterOptional.isPresent()) {
                log.info("Submitting GTE in synchronous manner to Kafka reporter");
                kafkaReporterOptional.get().reportSynchronously();
                log.info("Submitted GTE to Kafka reporter");
            } else {
                log.error("No KafkaAvroEventKeyValueReporter found in container metrics for taskRunnerId: {} and applicationName: {}",
                    containerMetricsTaskRunnerId, containerMetricsApplicationName);
            }
        } else {
            log.error("Both {} and {} must be set to fetch container metrics instance for synchronous GTE submission",
                GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_CONTAINER_METRICS_TASK_RUNNER_ID,
                GobblinTemporalConfigurationKeys.GOBBLIN_TEMPORAL_CONTAINER_METRICS_APPLICATION_NAME);
        }
    }

    private static String summarizeEventMetadataForLogging(GobblinEventBuilder eventBuilder) {
        Map<String, String> metadata = eventBuilder.getMetadata();
        return String.format("name: '%s'; namespace: '%s'; type: %s; start: %s; end: %s",
            eventBuilder.getName(),
            eventBuilder.getNamespace(),
            metadata.getOrDefault(EventSubmitter.EVENT_TYPE, "<<event type not indicated>>"),
            metadata.getOrDefault(TimingEvent.METADATA_START_TIME, "<<no start time>>"),
            metadata.getOrDefault(TimingEvent.METADATA_END_TIME, "<<no end time>>"));
    }
}
