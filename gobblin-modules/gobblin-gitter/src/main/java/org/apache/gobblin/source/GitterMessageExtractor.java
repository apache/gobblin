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
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.converter.DataConversionException;
import org.apache.gobblin.converter.avro.JsonRecordAvroSchemaToAvroConverter;
import org.apache.gobblin.password.PasswordManager;
import org.apache.gobblin.source.extractor.DataRecordException;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.extract.BigIntegerWatermark;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.google.common.io.Closer;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;


/**
 * Extractor to extract Gitter Messages.
 */
@Slf4j
public class GitterMessageExtractor implements Extractor<Schema, GenericRecord> {

  protected static final String GITTER_ROOM_NAMES_KEY = "source.gitter.roomNames";
  protected static final String GITTER_EXTRACTOR_KEY = "source.gitter.extractorClass";
  protected static final String GITTER_EXTRACTOR_DEFAULT = "org.apache.gobblin.source.GitterMessageExtractor";
  protected static final String GITTER_REST_API_ENDPOINT_KEY = "source.gitter.restEndPoint";
  protected static final String GITTER_REST_API_ENDPOINT_DEFAULT = "https://api.gitter.im/v1/rooms";

  protected static final Gson gson = new Gson();
  protected final Closer closer = Closer.create();
  protected final WorkUnitState workUnitState;
  protected final WorkUnit workUnit;
  protected final String gitterKey;
  protected Schema gitterMessageSchema;
  protected BigInteger gitterRollingWatermark;

  protected final Queue<GenericRecord> gitterMessages;

  protected final String gitterRoom;
  protected String gitterRoomId;
  protected final String gitterRestEndpoint;
  protected final HttpClient httpClient;

  public GitterMessageExtractor(WorkUnitState workUnitState) {
    this.workUnitState = workUnitState;
    this.workUnit = this.workUnitState.getWorkunit();
    this.gitterKey = PasswordManager.getInstance(this.workUnitState)
        .readPassword(this.workUnitState.getProp(ConfigurationKeys.SOURCE_CONN_PASSWORD));
    this.gitterRestEndpoint = this.workUnitState.getProp(GITTER_REST_API_ENDPOINT_KEY, GITTER_REST_API_ENDPOINT_DEFAULT);
    this.gitterRoom = workUnitState.getProp(ConfigurationKeys.DATASET_URN_KEY);
    this.gitterRollingWatermark = this.workUnitState.getWorkunit()
        .getLowWatermark(BigIntegerWatermark.class, new Gson()).getValue();
    log.info(String.format("Going to pull from Gitter room: %s from message Id: %s represented by watermark: %s",
        this.gitterRoom, this.gitterRollingWatermark.toString(16), this.gitterRollingWatermark));
    this.gitterMessages = new PriorityQueue<>();

    // Create HttpClient and fetch Room Id
    try {
      this.httpClient = this.closer.register(HttpClientBuilder.create().build());
      HttpGet httpGet = new HttpGet(this.gitterRestEndpoint);
      httpGet.addHeader("Accept", "application/json");
      httpGet.addHeader("Authorization", String.format("Bearer %s", this.gitterKey));

      HttpResponse response = httpClient.execute(httpGet);
      int responseCode = response.getStatusLine().getStatusCode();
      switch (responseCode) {
        case 200: {
          // Parse response and extract Room Id
          String roomsResponseStr = EntityUtils.toString(response.getEntity());
          if (null == roomsResponseStr) {
            log.error(String.format("Could not fetch Room Id for specified room: %s "
                + "Got null response", this.gitterRoom));
            throw new RuntimeException(String.format("Could not fetch Room Id for specified room: %s "
                + "Got null response", this.gitterRoom));
          }
          JsonParser parser = new JsonParser();
          JsonArray roomsResponse = parser.parse(roomsResponseStr).getAsJsonArray();

          for (JsonElement roomResponse : roomsResponse) {
            // Name should always be present, so NPE should happen if well-formed response
            if (this.gitterRoom.equals(roomResponse.getAsJsonObject().get("name").getAsString())) {
              this.gitterRoomId = roomResponse.getAsJsonObject().get("id").getAsString();
              break;
            }
          }
          if (null == this.gitterRoomId) {
            log.error(String.format("Could not fetch Room Id for specified room: %s "
                + "Room not found in response: %s", this.gitterRoom, roomsResponseStr));
            throw new RuntimeException(String.format("Could not fetch Room Id for specified room: %s "
                + "Room not found in response: %s", this.gitterRoom, roomsResponseStr));
          }
          log.info(String.format("Obtained Gitter message room id: %s", this.gitterRoomId));
          break;
        }
        default: {
          log.error(String.format("Could not fetch Room Id for specified room: %s "
              + "Got response code: %s", this.gitterRoom, responseCode));
          throw new RuntimeException(String.format("Could not fetch Room Id for specified room: %s "
              + "Got response code: %s", this.gitterRoom, responseCode));
        }
      }
    } catch (IOException | ParseException ex) {
      log.error(String.format("Could not fetch Room Id for specified room: %s ", this.gitterRoom), ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Schema getSchema()
      throws IOException {
    // Use schema from config if provided, else use default Gitter schema
    String schemaString = IOUtils.toString(this.getClass().getResourceAsStream("/gitter/message.avsc"),
        StandardCharsets.UTF_8);
    this.gitterMessageSchema = new Schema.Parser().parse(workUnit.getProp(ConfigurationKeys.SOURCE_SCHEMA, schemaString));
    return this.gitterMessageSchema;
  }

  @Nullable
  @Override
  public GenericRecord readRecord(GenericRecord reuse)
      throws DataRecordException, IOException {
    if (gitterMessages.size() == 0) {
      // We fetch Gitter messages in batches / pages because of two reasons:
      // 1. There is no way to easily figure out id for next message to pull, you can only pull in batch reliably.
      // 2. It is anyway optimal (lesser HTTP calls) to pull in batches.
      fetchGitterMessages(this.gitterMessages);
      if (gitterMessages.size() == 0) {
        // No more messages left to fetch, finish
        return null;
      }
    }

    // Keep moving rolling watermark for messages consumed, so that next time pagination hits
    // .. we continue from there.
    GenericRecord message = this.gitterMessages.poll();
    this.gitterRollingWatermark = new BigInteger(message.get("id").toString(), 16);
    this.workUnitState.setActualHighWatermark(new BigIntegerWatermark(this.gitterRollingWatermark));

    return message;
  }

  protected void fetchGitterMessages(Collection<GenericRecord> messageCollection) {
    // Fetch messages from Gitter
    try {
      log.info(String.format("Fetching from watermark: %s", this.gitterRollingWatermark));
      HttpGet httpGet;
      if (BigInteger.ZERO.equals(this.gitterRollingWatermark)) {
        httpGet = new HttpGet(String.format("%s/%s/chatMessages", this.gitterRestEndpoint, this.gitterRoomId));
      } else {
        httpGet = new HttpGet(String.format("%s/%s/chatMessages/?afterId=%s", this.gitterRestEndpoint, this.gitterRoomId,
            this.gitterRollingWatermark.toString(16)));
      }
      httpGet.addHeader("Accept", "application/json");
      httpGet.addHeader("Authorization", String.format("Bearer %s", this.gitterKey));

      HttpResponse response = httpClient.execute(httpGet);
      int responseCode = response.getStatusLine().getStatusCode();
      switch (responseCode) {
        case 200: {
          // Parse response and get Messages
          String messagesResponseStr = EntityUtils.toString(response.getEntity());
          log.info(String.format("Got response: %s", messagesResponseStr));
          if (null == messagesResponseStr) {
            log.error(String.format("Could not fetch messages for room: %s "
                + "Got null response", this.gitterRoom));
            throw new RuntimeException(String.format("Could not fetch messages for room: %s "
                + "Got null response", this.gitterRoom));
          }
          JsonParser parser = new JsonParser();
          JsonArray messagesResponse = parser.parse(messagesResponseStr).getAsJsonArray();

          // Go over each message and add it to list
          for (JsonElement messageResponse : messagesResponse) {
            log.info(String.format("Got message in json: %s", messageResponse));
            JsonRecordAvroSchemaToAvroConverter<String> converter = new JsonRecordAvroSchemaToAvroConverter<>();
            converter.init(workUnitState);
            try {
              GenericRecord messageAvro = converter.convertRecord(this.gitterMessageSchema,
                  messageResponse.getAsJsonObject(), this.workUnitState).iterator().next();
              if (null != messageAvro) {
                log.info(String.format("Got message in avro: %s", messageResponse));
                messageCollection.add(messageAvro);
              } else {
                log.error(String.format("Could not convert message: %s to Avro. Got null. Bailing out.", messageResponse));
                throw new RuntimeException(String.format("Could not convert message: %s to Avro. Got null. Bailing out.",
                    messageResponse));
              }
            } catch (DataConversionException dce) {
              log.error(String.format("Could not convert message: %s to Avro. Bailing out.", messageResponse), dce);
              throw new RuntimeException(dce);
            }
          }
          break;
        }
        default: {
          log.error(String.format("Could not fetch messages for room: %s "
              + "Got response code: %s", this.gitterRoom, responseCode));
          throw new RuntimeException(String.format("Could not fetch messages for room: %s "
              + "Got response code: %s", this.gitterRoom, responseCode));
        }
      }
    } catch (IOException | ParseException ex) {
      log.error(String.format("Could not fetch messages for room: %s ", this.gitterRoom), ex);
      throw new RuntimeException(ex);
    }
  }

  @Override
  public long getExpectedRecordCount() {
    // We don't know this in advance
    return 0;
  }

  @Override
  public long getHighWatermark() {
    // Watermark management will be handled in terms of non-numeric Ids
    return 0;
  }

  @Override
  public void close()
      throws IOException {
    try {
      this.closer.close();
    } catch (IOException ioe) {
      log.error("Failed to close the resources", ioe);
      throw ioe;
    }
  }
}
