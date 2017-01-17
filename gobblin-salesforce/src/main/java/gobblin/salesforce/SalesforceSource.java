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

package gobblin.salesforce;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.source.extractor.Extractor;
import gobblin.source.extractor.exception.ExtractPrepareException;
import gobblin.source.extractor.exception.RestApiConnectionException;
import gobblin.source.extractor.exception.RestApiProcessingException;
import gobblin.source.extractor.extract.Command;
import gobblin.source.extractor.extract.CommandOutput;
import gobblin.source.extractor.extract.QueryBasedSource;
import gobblin.source.extractor.extract.restapi.RestApiConnector;
import lombok.extern.slf4j.Slf4j;


/**
 * An implementation of {@link QueryBasedSource} for salesforce data sources.
 */
@Slf4j
public class SalesforceSource extends QueryBasedSource<JsonArray, JsonElement> {

  public static final String USE_ALL_OBJECTS = "use.all.objects";
  public static final boolean DEFAULT_USE_ALL_OBJECTS = false;

  @Override
  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state) throws IOException {
    try {
      return new SalesforceExtractor(state).build();
    } catch (ExtractPrepareException e) {
      log.error("Failed to prepare extractor", e);
      throw new IOException(e);
    }
  }

  protected Set<SourceEntity> getSourceEntities(State state) {
    if (!state.getPropAsBoolean(USE_ALL_OBJECTS, DEFAULT_USE_ALL_OBJECTS)) {
      return super.getSourceEntities(state);
    }

    SalesforceConnector connector = new SalesforceConnector(state);
    try {
      if (!connector.connect()) {
        throw new RuntimeException("Failed to connect.");
      }
    } catch (RestApiConnectionException e) {
      throw new RuntimeException("Failed to connect.", e);
    }

    List<Command> commands = RestApiConnector.constructGetCommand(connector.getFullUri("/sobjects"));
    try {
      CommandOutput<?, ?> response = connector.getResponse(commands);
      Iterator<String> itr = (Iterator<String>) response.getResults().values().iterator();
      if (itr.hasNext()) {
        String next = itr.next();
        return getSourceEntities(next);
      }
      throw new RuntimeException("Unable to retrieve source entities");
    } catch (RestApiProcessingException e) {
      throw Throwables.propagate(e);
    }

  }

  private static Set<SourceEntity> getSourceEntities(String response) {
    Set<SourceEntity> result = Sets.newHashSet();
    JsonObject jsonObject = new Gson().fromJson(response, JsonObject.class).getAsJsonObject();
    JsonArray array = jsonObject.getAsJsonArray("sobjects");
    for (JsonElement element : array) {
      String sourceEntityName = element.getAsJsonObject().get("name").getAsString();
      result.add(SourceEntity.fromSourceEntityName(sourceEntityName));
    }
    return result;
  }

}
