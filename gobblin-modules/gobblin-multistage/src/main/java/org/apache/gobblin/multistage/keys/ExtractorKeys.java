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
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.gobblin.configuration.State;
import org.apache.gobblin.multistage.configuration.MultistageProperties;
import org.apache.gobblin.multistage.preprocessor.StreamProcessor;
import org.apache.gobblin.multistage.util.JsonSchema;
import org.apache.gobblin.source.workunit.WorkUnit;
import org.joda.time.DateTime;

/**
 * each of these keys provide information how to populate corresponding values
 *
 * each format Extractor is responsible for populating these key with proper values
 * so that their those values can be pull by the Source
 *
 * @author chrli
 */
@Slf4j
@Getter (AccessLevel.PUBLIC)
@Setter
public class ExtractorKeys {
  final static private List<MultistageProperties> ESSENTIAL_PARAMETERS = Lists.newArrayList(
      MultistageProperties.EXTRACT_TABLE_NAME_KEY,
      MultistageProperties.MSTAGE_ACTIVATION_PROPERTY,
      MultistageProperties.MSTAGE_PARAMETERS
  );

  private JsonObject activationParameters = new JsonObject();
  private long startTime = DateTime.now().getMillis();
  private long delayStartTime;
  private String signature;
  private JsonSchema inferredSchema = null;
  private String sessionKeyValue;
  private List<StreamProcessor<?>> preprocessors = new ArrayList<>();
  private JsonObject dynamicParameters = new JsonObject();

  public void logDebugAll(WorkUnit workUnit) {
    log.debug("These are values in MultistageExtractor regarding to Work Unit: {}",
        workUnit == null ? "testing" : workUnit.getProp(MultistageProperties.DATASET_URN_KEY.toString()));
    log.debug("Activation parameters: {}", activationParameters);
    log.debug("Starting time: {}", startTime);
    log.debug("Signature of the work unit: {}", signature);
    if (inferredSchema != null) {
      log.info("Inferred schema: {}", inferredSchema.toString());
      log.info("Avro-flavor schema: {}", inferredSchema.getAltSchema(new HashMap<>(), false).toString());
    }
    log.debug("Session Status: {}", sessionKeyValue);
  }

  public void logUsage(State state) {
    for (MultistageProperties p: ESSENTIAL_PARAMETERS) {
      log.info("Property {} ({}) has value {} ", p.toString(), p.getClassName(), p.getValidNonblankWithDefault(state));
    }
  }
}
