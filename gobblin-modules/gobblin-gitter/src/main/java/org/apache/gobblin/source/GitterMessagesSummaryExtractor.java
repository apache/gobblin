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

package org.apache.gobblin.source;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.extract.BigIntegerWatermark;

import com.google.common.collect.Lists;

/**
 * Extractor to extract Gitter Messages.
 */
@Slf4j
public class GitterMessagesSummaryExtractor extends GitterMessageExtractor {

  private Schema gitterMessagesSummarySchema;

  public GitterMessagesSummaryExtractor(WorkUnitState workUnitState) {
    super(workUnitState);
  }

  @Override
  public Schema getSchema()
      throws IOException {
    // Required to initialize per-message schema
    super.getSchema();
    String schemaString = IOUtils.toString(this.getClass().getResourceAsStream("/gitter/messages_summary_avsc.avsc"),
        StandardCharsets.UTF_8);
    this.gitterMessagesSummarySchema = new Schema.Parser().parse(schemaString);
    return this.gitterMessagesSummarySchema;
  }

  @Nullable
  @Override
  public GenericRecord readRecord(GenericRecord reuse)
      throws DataRecordException, IOException {
    // Fetch all batches / pages of messages
    long preFetchSize, postFetchSize;
    log.info("Starting to fetch messages from Gitter to generate summary.");
    do {
      preFetchSize = this.gitterMessages.size();
      fetchGitterMessages(this.gitterMessages);
      postFetchSize = this.gitterMessages.size();
      log.info(String.format("Fetching messages pre-fetch size: %s and post-fetch size: %s", preFetchSize, postFetchSize));
    } while (postFetchSize > preFetchSize);

    return generateMessageSummary(this.gitterMessages);
  }

  private GenericRecord generateMessageSummary(Queue<GenericRecord> gitterMessages) {
    if (this.gitterMessages.size() == 0) {
      return null;
    }
    GenericRecord messageSummary = new GenericData.Record(this.gitterMessagesSummarySchema);

    String firstMessageDateTime = StringUtils.EMPTY, lastMessageDateTime = StringUtils.EMPTY;
    String firstMessageId = StringUtils.EMPTY, lastMessageId = StringUtils.EMPTY;
    GenericRecord currentRecord = null;
    List<String> textMessages = Lists.newArrayList();
    List<String> htmlMessages = Lists.newArrayList();

    log.info(String.format("Generating summary for %s messages", this.gitterMessages.size()));
    while (!this.gitterMessages.isEmpty()) {
      currentRecord = this.gitterMessages.poll();
      if (firstMessageDateTime.length() == 0) {
        firstMessageDateTime = currentRecord.get("sent").toString();
        firstMessageId = currentRecord.get("id").toString();
      }
      String displayName = ((GenericRecord) currentRecord.get("fromUser")).get("displayName").toString();
      textMessages.add(String.format("%s wrote at %s : %s", currentRecord.get(displayName), currentRecord.get("sent"),
          currentRecord.get("text")));
      textMessages.add(String.format("<b>%s wrote at %s :<b><br> %s", currentRecord.get(displayName), currentRecord.get("sent"),
          currentRecord.get("html")));
    }
    if (null != currentRecord) {
      lastMessageDateTime = currentRecord.get("sent").toString();
      lastMessageId = currentRecord.get("id").toString();
      this.gitterRollingWatermark = new BigInteger(currentRecord.get("id").toString(), 16);
      this.workUnitState.setActualHighWatermark(new BigIntegerWatermark(this.gitterRollingWatermark));
    }
    messageSummary.put("startId", firstMessageId);
    messageSummary.put("lastId", lastMessageId);
    messageSummary.put("text", StringUtils.join(textMessages, "\n"));
    messageSummary.put("html", StringUtils.join(htmlMessages, "<br>"));
    messageSummary.put("title", String.format("%s Gitter room messages for: %s to %s", this.gitterRoom,
        firstMessageDateTime, lastMessageDateTime));
    log.info(String.format("Message summary generated: %s", messageSummary));

    return messageSummary;
  }
}
