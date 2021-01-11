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

package org.apache.gobblin.source.extractor.extract.jdbc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import java.io.IOException;
import java.net.URI;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.configuration.SourceState;
import org.apache.gobblin.configuration.WorkUnitState;
import org.apache.gobblin.dataset.DatasetConstants;
import org.apache.gobblin.dataset.DatasetDescriptor;
import org.apache.gobblin.source.extractor.Extractor;
import org.apache.gobblin.source.extractor.exception.ExtractPrepareException;
import org.apache.gobblin.source.extractor.extract.QueryBasedSource;
import org.apache.gobblin.source.jdbc.MysqlExtractor;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of mysql source to get work units
 *
 * @author nveeramr
 */
public class MysqlSource extends QueryBasedSource<JsonArray, JsonElement> {
  private static final Logger LOG = LoggerFactory.getLogger(MysqlSource.class);

  @Override
  public Extractor<JsonArray, JsonElement> getExtractor(WorkUnitState state) throws IOException {
    Extractor<JsonArray, JsonElement> extractor = null;
    try {
      extractor = new MysqlExtractor(state).build();
    } catch (ExtractPrepareException e) {
      LOG.error("Failed to prepare extractor: error - " + e.getMessage());
      throw new IOException(e);
    }
    return extractor;
  }

  protected void addLineageSourceInfo(SourceState sourceState, SourceEntity entity, WorkUnit workUnit) {
    String host = sourceState.getProp(ConfigurationKeys.SOURCE_CONN_HOST_NAME);
    String port = sourceState.getProp(ConfigurationKeys.SOURCE_CONN_PORT);
    String database = sourceState.getProp(ConfigurationKeys.SOURCE_QUERYBASED_SCHEMA);
    String serverUrl = "mysql://" + host.trim() + ":" + port;
    String connectionUrl = "jdbc:" + serverUrl + "/" + database.trim();
    DatasetDescriptor source = new DatasetDescriptor(DatasetConstants.PLATFORM_MYSQL, URI.create(serverUrl),
        database + "." + entity.getSourceEntityName());
    source.addMetadata(DatasetConstants.CONNECTION_URL, connectionUrl);
    if (lineageInfo.isPresent()) {
      lineageInfo.get().setSource(source, workUnit);
    }
  }
}
