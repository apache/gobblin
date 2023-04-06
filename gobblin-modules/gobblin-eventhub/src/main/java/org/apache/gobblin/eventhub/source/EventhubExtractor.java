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

import com.google.gson.JsonObject;
import com.microsoft.azure.eventhubs.*;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.LongWatermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link org.apache.gobblin.source.extractor.Extractor}  implementation for MS EventHub source.
 * Each EventhubExtractor processes one source partition.
 */
public class EventhubExtractor implements Extractor<JsonObject, EventData> {

    public static final Logger LOG = LoggerFactory.getLogger(EventhubExtractor.class);

    public static final String CONFIG_PREFIX = "source.azure.eventhubs.";
    public static final String ENDPOINT_KEY = CONFIG_PREFIX + "endpoint";
    public static final String EVENTHUB_NAME_KEY = CONFIG_PREFIX + "name";
    public static final String SAS_KEY_NAME_KEY = CONFIG_PREFIX + "sas.key.name";
    public static final String SAS_KEY_KEY = CONFIG_PREFIX + "sas.key";
    public static final String CONSUMER_GROUP_KEY = CONFIG_PREFIX + "consumer.group";
    public static final String MAX_EXTRACTION_DURATION_KEY = CONFIG_PREFIX + "max.extraction.duration";
    public static final String MAX_EVENTS_IN_BATCH_KEY = CONFIG_PREFIX + "max.events.in.batch";
    public static final String SCHEMA_CLASS = CONFIG_PREFIX + "schema.class";
    public static final int DEFAULT_MAX_EVENTS_IN_BATCH = 100;

    private WorkUnitState workUnitState;
    private String highWatermark;
    private PartitionReceiver receiver;
    private EventHubClient ehClient;
    private ScheduledExecutorService executorService;
    private int eventsInBatch;
    private Instant startTime; // for logging only
    private Duration maxDuration; // for logging only
    private Instant endTime;
    private Iterator<EventData> iterator;
    private int pulled;
    private EventHubSchema schemaProvider;
    // TODO codahale metrics.

    public EventhubExtractor(WorkUnitState workUnitState) {
        this.workUnitState = workUnitState;
        String endpoint = workUnitState.getProp(ENDPOINT_KEY);
        String eventHubName = workUnitState.getProp(EVENTHUB_NAME_KEY);
        String sasKeyName = workUnitState.getProp(SAS_KEY_NAME_KEY);
        String sasKey = workUnitState.getProp(SAS_KEY_KEY);
        String consumerGroup = workUnitState.getProp(CONSUMER_GROUP_KEY, EventHubClient.DEFAULT_CONSUMER_GROUP_NAME);
        LOG.info(String.format("Connecting to event hub: %s, name: %s, SAS key name: %s", endpoint, eventHubName, sasKeyName));
        eventsInBatch = workUnitState.getPropAsInt(MAX_EVENTS_IN_BATCH_KEY, DEFAULT_MAX_EVENTS_IN_BATCH);
        String duration = workUnitState.getProp(MAX_EXTRACTION_DURATION_KEY);
        startTime = Instant.now();
        maxDuration = Duration.parse(duration);
        endTime = startTime.plus(maxDuration);
        long watermark = workUnitState.getWorkunit().getLowWatermark(LongWatermark.class).getValue();
        pulled = 0;
        try {
            schemaProvider = getClass().getClassLoader()
                    .loadClass(workUnitState.getProp(SCHEMA_CLASS))
                    .asSubclass(EventHubSchema.class)
                    .getConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        }
        try {
            URI endPoint;
            try {
                endPoint = new URI(endpoint);
            } catch (URISyntaxException ex) {
                throw new RuntimeException(ex);
            }

            final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                    .setEndpoint(endPoint)
                    .setEventHubName(eventHubName)
                    .setSasKeyName(sasKeyName)
                    .setSasKey(sasKey);

            executorService = Executors.newSingleThreadScheduledExecutor();
            ehClient = EventHubClient.createSync(connStr.toString(), executorService);
            LOG.info(String.format("Using consumer group: %s", consumerGroup));
            receiver = ehClient.createEpochReceiverSync(
                    consumerGroup,
                    workUnitState.getProp(EventhubSource.CURRENT_PARTITION_KEY),
                    watermark == ConfigurationKeys.DEFAULT_WATERMARK_VALUE ? EventPosition.fromStartOfStream() : EventPosition.fromOffset(Long.toString(watermark)),
                    1);
            LOG.info(String.format("Extraction parameters: max events in batch = %s, duration = %s -> end time = %s",
                    eventsInBatch, duration, endTime));
        } catch (EventHubException | IOException e) {
            throw new RuntimeException(e);
        }
    }

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
            if (endTime != null && Instant.now().isBefore(endTime)) {
                LOG.info("Attempting to receive records from source.");
                Iterable<EventData> received = receiver.receiveSync(eventsInBatch);
                if (received != null) {
                    iterator = received.iterator();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Received batch size: %s", received.spliterator().getExactSizeIfKnown()));
                    }
                } else if (LOG.isDebugEnabled()) {
                    LOG.debug("No records received from source.");
                }
            } else if (LOG.isDebugEnabled()) {
                LOG.debug("Reached scheduled end time of the extraction: " + endTime);
            }
        }
        return iterator;
    }

    @Override
    public JsonObject getSchema() throws IOException {
        return schemaProvider.getSchema(workUnitState);
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
        } catch (EventHubException ex) {
            throw new RuntimeException(ex);
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
            this.workUnitState.setActualHighWatermark(longWatermark);
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
        } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
    }
}

