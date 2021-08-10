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

package org.apache.gobblin.multistage.keys;

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Iterator;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.ConfigurationKeys;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.source.workunit.WorkUnit;


/**
 * These attributes are defined and maintained in JsonExtractor
 *
 * @author chrli
 */
@Slf4j
@Getter(AccessLevel.PUBLIC)
@Setter
public class JsonExtractorKeys {
  private Iterator<JsonElement> jsonElementIterator = null;
  private long processedCount;
  private long totalCount;
  private long currentPageNumber = 0;
  private JsonObject pushDowns;

  private List<MultistageProperties> essentialParameters = Lists.newArrayList(
      MultistageProperties.MSTAGE_DATA_FIELD,
      MultistageProperties.MSTAGE_TOTAL_COUNT_FIELD);

  public void logDebugAll(WorkUnit workUnit) {
    log.debug("These are values of JsonExtractor regarding to Work Unit: {}",
        workUnit == null ? "testing" : workUnit.getProp(ConfigurationKeys.DATASET_URN_KEY));
    log.debug("Total rows expected or processed: {}", totalCount);
    log.debug("Total rows processed: {}", processedCount);
  }
}
