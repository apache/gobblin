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

import io.temporal.workflow.Workflow;

import org.slf4j.Logger;

import org.apache.gobblin.metrics.event.EventSubmitter;
import org.apache.gobblin.metrics.event.GobblinEventBuilder;
import org.apache.gobblin.metrics.event.TimingEvent;


public class SubmitGTEActivityImpl implements SubmitGTEActivity {
    private static Logger log = Workflow.getLogger(SubmitGTEActivityImpl.class);

    @Override
    public void submitGTE(GobblinEventBuilder eventBuilder, EventSubmitterContext eventSubmitterContext) {
        log.info("submitting GTE - {}", summarizeEventMetadataForLogging(eventBuilder));
        eventSubmitterContext.create().submit(eventBuilder);
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
