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

package org.apache.gobblin.runtime.api;

import java.io.IOException;
import java.net.URI;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Splitter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.service.FlowId;


/**
 * This is a class to package all the parameters that should be used to search {@link FlowSpec} in a {@link SpecStore}
 */
@Getter
@Builder
@ToString
@AllArgsConstructor
@Slf4j
public class FlowSpecSearchObject implements SpecSearchObject {
  private final URI flowSpecUri;
  private final String flowGroup;
  private final String flowName;
  private final String templateURI;
  private final String userToProxy;
  private final String sourceIdentifier;
  private final String destinationIdentifier;
  private final String schedule;
  private final String modifiedTimestamp;
  private final Boolean isRunImmediately;
  private final String owningGroup;
  private final String propertyFilter;
  private final int start;
  private final int count;

  public static FlowSpecSearchObject fromFlowId(FlowId flowId) {
    return FlowSpecSearchObject.builder().flowGroup(flowId.getFlowGroup()).flowName(flowId.getFlowName()).build();
  }

  /** This expects at least one parameter of `this` to be not null */
  @Override
  public String augmentBaseGetStatement(String baseStatement)
      throws IOException {
    List<String> conditions = new ArrayList<>();
    List<String> limitAndOffset = new ArrayList<>();

    /*
     * IMPORTANT: the order of `conditions` added must align with the order of parameter binding later in `completePreparedStatement`!
     */

    if (this.getFlowSpecUri() != null) {
      conditions.add("spec_uri = ?");
    }

    if (this.getFlowGroup() != null) {
      conditions.add("flow_group = ?");
    }

    if (this.getFlowName() != null) {
      conditions.add("flow_name = ?");
    }

    if (this.getTemplateURI() != null) {
      conditions.add("template_uri = ?");
    }

    if (this.getUserToProxy() != null) {
      conditions.add("user_to_proxy = ?");
    }

    if (this.getSourceIdentifier() != null) {
      conditions.add("source_identifier = ?");
    }

    if (this.getDestinationIdentifier() != null) {
      conditions.add("destination_identifier = ?");
    }

    if (this.getSchedule() != null) {
      conditions.add("schedule = ?");
    }

    if (this.getModifiedTimestamp() != null) {
      conditions.add("modified_time = ?");
    }

    if (this.getIsRunImmediately() != null) {
      conditions.add("isRunImmediately = ?");
    }

    if (this.getOwningGroup() != null) {
      conditions.add("owning_group = ?");
    }

    if (this.getCount() > 0) {
      // Order by two fields to make a full order by
      limitAndOffset.add(" ORDER BY modified_time DESC, spec_uri ASC LIMIT ?");
      if (this.getStart() > 0) {
        limitAndOffset.add(" OFFSET ?");
      }
    }

    // If the propertyFilter is myKey=myValue, it looks for a config where key is `myKey` and value contains string `myValue`.
    // If the propertyFilter string does not have `=`, it considers the string as a key and just looks for its existence.
    // Multiple occurrences of `=` in  propertyFilter are not supported and ignored completely.
    if (this.getPropertyFilter() != null) {
      String propertyFilter = this.getPropertyFilter();
      Splitter commaSplitter = Splitter.on(",").trimResults().omitEmptyStrings();
      for (String property : commaSplitter.splitToList(propertyFilter)) {
        if (property.contains("=")) {
          String[] keyValue = property.split("=");
          if (keyValue.length != 2) {
            log.error("Incorrect flow config search query");
            continue;
          }
          conditions.add("spec_json->'$.configAsProperties.\"" + keyValue[0] + "\"' like " + "'%" + keyValue[1] + "%'");
        } else {
          conditions.add("spec_json->'$.configAsProperties.\"" + property + "\"' is not null");
        }
      }
    }

    if (conditions.size() == 0 && limitAndOffset.size() == 0) {
      throw new IOException("At least one condition is required to query flow configs.");
    }
    return baseStatement + String.join(" AND ", conditions) + String.join(" ", limitAndOffset);
  }

  @Override
  public void completePreparedStatement(PreparedStatement statement)
      throws SQLException {
    int i = 0;

    /*
     * IMPORTANT: the order of binding params must align with the order of building the conditions earlier in `augmentBaseGetStatement`!
     */

    if (this.getFlowSpecUri() != null) {
      statement.setString(++i, this.getFlowSpecUri().toString());
    }

    if (this.getFlowGroup() != null) {
      statement.setString(++i, this.getFlowGroup());
    }

    if (this.getFlowName() != null) {
      statement.setString(++i, this.getFlowName());
    }

    if (this.getTemplateURI() != null) {
      statement.setString(++i, this.getTemplateURI());
    }

    if (this.getUserToProxy() != null) {
      statement.setString(++i, this.getUserToProxy());
    }

    if (this.getSourceIdentifier() != null) {
      statement.setString(++i, this.getSourceIdentifier());
    }

    if (this.getDestinationIdentifier() != null) {
      statement.setString(++i, this.getDestinationIdentifier());
    }

    if (this.getSchedule() != null) {
      statement.setString(++i, this.getSchedule());
    }

    if (this.getIsRunImmediately() != null) {
      statement.setBoolean(++i, this.getIsRunImmediately());
    }

    if (this.getOwningGroup() != null) {
      statement.setString(++i, this.getOwningGroup());
    }

    if (this.getCount() > 0) {
      statement.setInt(++i, this.getCount());
      if (this.getStart() > 0) {
        statement.setInt(++i, this.getStart());
      }
    }
  }
}
