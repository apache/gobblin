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
package org.apache.gobblin.eventhub.source;

import com.microsoft.azure.eventhubs.*;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by Jan on 23-Jan-19.
 */
public class EventhubExtractor implements Extractor<Void, EventData> {

    public static final Logger LOG = LoggerFactory.getLogger(EventhubExtractor.class);

    public static final String CONFIG_PREFIX = "source.azure.eventhubs.";
    public static final String ENDPOINT_KEY = CONFIG_PREFIX + "endpoint";
    public static final String EVENTHUB_NAME_KEY = CONFIG_PREFIX + "name";
    public static final String SAS_KEY_NAME_KEY = CONFIG_PREFIX + "sas.key.name";
    public static final String SAS_KEY_KEY = CONFIG_PREFIX + "sas.key";
    public static final String CONSUMER_GROUP_KEY = CONFIG_PREFIX + "consumer.group";
    public static final String MAX_EXTRACTION_DURATION_KEY = CONFIG_PREFIX + "max.extraction.duration";
    public static final String MAX_EVENTS_IN_BATCH_KEY = CONFIG_PREFIX + "max.events.in.batch";
    public static final int DEFAULT_MAX_EVENTS_IN_BATCH = 100;

    private WorkUnitState workUnit;
    private String highWatermark;
    private PartitionReceiver receiver;
    private EventHubClient ehClient;
    private ScheduledExecutorService executorService;
    private int eventsInBatch;
    private LocalDateTime startTime; // for logging only
    private Duration maxDuration; // for logging only
    private LocalDateTime endTime;
    private Iterator<EventData> iterator;
    private int pulled;

    private void logStatus() {
        LOG.info(String.format("Current high watermark: %s, job duration so far: %s (of max %s), received events cummulative: %d",
                highWatermark,
                Duration.between(startTime, LocalDateTime.now()).toString(),
                maxDuration.toString(),
                pulled));
    }

    private Iterator<EventData> getEventIterator() throws EventHubException {
        if (iterator == null || !iterator.hasNext()) {
            logStatus();
            iterator = null;
            if (endTime != null && LocalDateTime.now(ZoneId.of("UTC")).isBefore(endTime)) {
                LOG.info("Attempting to receive records from source.");
                Iterable<EventData> received = receiver.receiveSync(eventsInBatch);
                if (received != null) {
                    iterator = received.iterator();
                    LOG.info(String.format("Received batch size: %s", received.spliterator().getExactSizeIfKnown()));
                } else {
                    LOG.info("No records received from source.");
                }
            } else {
                LOG.info("Reached scheduled end time of the extraction: " + endTime);
            }
        }
        return iterator;
    }

    public EventhubExtractor(WorkUnitState workUnit) {
        this.workUnit = workUnit;
        String endpoint = workUnit.getProp(ENDPOINT_KEY);
        String eventHubName = workUnit.getProp(EVENTHUB_NAME_KEY);
        String sasKeyName = workUnit.getProp(SAS_KEY_NAME_KEY);
        String sasKey = workUnit.getProp(SAS_KEY_KEY);
        String consumerGroup = workUnit.getProp(CONSUMER_GROUP_KEY, EventHubClient.DEFAULT_CONSUMER_GROUP_NAME);
        LOG.info(String.format("Connecting to event hub: %s, name: %s, SAS key name: %s", endpoint, eventHubName, sasKeyName));
        eventsInBatch = workUnit.getPropAsInt(MAX_EVENTS_IN_BATCH_KEY, DEFAULT_MAX_EVENTS_IN_BATCH);
        String duration = workUnit.getProp(MAX_EXTRACTION_DURATION_KEY);
        startTime = LocalDateTime.now();
        maxDuration = Duration.parse(duration);
        endTime = startTime.plus(maxDuration);
        long watermark = workUnit.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
        pulled = 0;
        try {
            URI endPoint;
            try {
                endPoint = new URI(endpoint);
            } catch (URISyntaxException ex) {
                ex.printStackTrace();
                return;
            }

            final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                    .setEndpoint(endPoint)
                    .setEventHubName(eventHubName)
                    .setSasKeyName(sasKeyName)
                    .setSasKey(sasKey);

            executorService = Executors.newSingleThreadScheduledExecutor();
            ehClient = EventHubClient.createSync(connStr.toString(), executorService);
            final EventHubRuntimeInformation eventHubInfo = ehClient.getRuntimeInformation().get();
            final String partitionId = eventHubInfo.getPartitionIds()[0]; // get first partition's id
            LOG.info(String.format("Using consumer group: %s", consumerGroup));
            receiver = ehClient.createEpochReceiverSync(
                    consumerGroup,
                    partitionId,
                    watermark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE ? EventPosition.fromStartOfStream() : EventPosition.fromOffset(Long.toString(watermark)),
                    1);
            LOG.info(String.format("Extraction parameters: max events in batch = %s, duration = %s -> end time = %s",
                    eventsInBatch, duration, endTime));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (EventHubException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Void getSchema() throws IOException {
        return null;
    }

    @Override
    public EventData readRecord(@Deprecated EventData d) throws DataRecordException, IOException {
        try {
            Iterator<EventData> iterator = getEventIterator();
            if (iterator != null) {
                EventData event = iterator.next();
                pulled++;
                highWatermark = event.getSystemProperties().getOffset();
                return event;
            } else {
                return null;
            }
        } catch (EventHubException e) {
            return null;
        }
    }

    @Override
    public long getExpectedRecordCount() {
        // TODO dummy implementation for now
        return 1;
    }

    @Override
    public long getHighWatermark() {
        return Long.parseLong(highWatermark);
    }

    @Override
    public void close() throws IOException {
        try {
            LongWatermark longWatermark = new LongWatermark(getHighWatermark());
            LOG.info("Persisting state of actual high watermark: " + longWatermark);
            this.workUnit.setActualHighWatermark(longWatermark);
        } catch (NumberFormatException ex) {
            LOG.info("Not persisting state of actual high watermark - invalid value: " + getHighWatermark());
        }
        try {
            LOG.info("Closing event hub receiver");
            receiver.close()
                    .thenComposeAsync(aVoid -> ehClient.close(), executorService)
                    .whenCompleteAsync((t, u) -> {
                        if (u != null) {
                            LOG.error(String.format("closing failed with error: %s", u.toString()));
                        }
                    }, executorService).get();

            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}

