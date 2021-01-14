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

package org.apache.gobblin.service.modules.spec;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.google.common.base.Optional;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.JobSpec;
import org.apache.gobblin.runtime.api.SpecExecutor;
import org.apache.gobblin.runtime.api.TopologySpec;
import org.apache.gobblin.service.ExecutionStatus;


@Slf4j
public class JobExecutionPlanListDeserializer implements JsonDeserializer<List<JobExecutionPlan>> {
  private final Map<URI,TopologySpec> topologySpecMap;

  public JobExecutionPlanListDeserializer(Map<URI, TopologySpec> topologySpecMap) {
    this.topologySpecMap = topologySpecMap;
  }

  /**
   * Gson invokes this call-back method during deserialization when it encounters a field of the
   * specified type.
   * <p>In the implementation of this call-back method, you should consider invoking
   * {@link JsonDeserializationContext#deserialize(JsonElement, Type)} method to create objects
   * for any non-trivial field of the returned object. However, you should never invoke it on the
   * the same type passing {@code json} since that will cause an infinite loop (Gson will call your
   * call-back method again).
   *
   * @param json The Json data being deserialized
   * @param typeOfT The type of the Object to deserialize to
   * @param context
   * @return a deserialized object of the specified type typeOfT which is a subclass of {@code T}
   * @throws JsonParseException if json is not in the expected format of {@code typeofT}
   */
  @Override
  public List<JobExecutionPlan> deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
      throws JsonParseException {
    List<JobExecutionPlan> jobExecutionPlans = new ArrayList<>();
    JsonArray jsonArray = json.getAsJsonArray();

    for (JsonElement jsonElement: jsonArray) {
      JsonObject serializedJobExecutionPlan = (JsonObject) jsonElement;
      JsonObject jobSpecJson = (JsonObject) serializedJobExecutionPlan.get(SerializationConstants.JOB_SPEC_KEY);
      JsonObject specExecutorJson = (JsonObject) serializedJobExecutionPlan.get(SerializationConstants.SPEC_EXECUTOR_KEY);
      ExecutionStatus executionStatus = ExecutionStatus.valueOf(serializedJobExecutionPlan.
          get(SerializationConstants.EXECUTION_STATUS_KEY).getAsString());

      String uri = jobSpecJson.get(SerializationConstants.JOB_SPEC_URI_KEY).getAsString();
      String version = jobSpecJson.get(SerializationConstants.JOB_SPEC_VERSION_KEY).getAsString();
      String description = jobSpecJson.get(SerializationConstants.JOB_SPEC_DESCRIPTION_KEY).getAsString();
      String templateURI = jobSpecJson.get(SerializationConstants.JOB_SPEC_TEMPLATE_URI_KEY).getAsString();
      String config = jobSpecJson.get(SerializationConstants.JOB_SPEC_CONFIG_KEY).getAsString();
      Config jobConfig = ConfigFactory.parseString(config);
      JobSpec jobSpec;
      try {
        JobSpec.Builder builder = (uri == null) ? JobSpec.builder() : JobSpec.builder(uri);
        builder = (templateURI == null) ? builder : builder.withTemplate(new URI(templateURI));
        builder = (version == null) ? builder : builder.withVersion(version);
        builder = (description == null) ? builder : builder.withDescription(description);
        jobSpec = builder.withConfig(jobConfig).build();
      } catch (URISyntaxException e) {
        log.error("Error deserializing JobSpec {}", config);
        throw new RuntimeException(e);
      }

      Config specExecutorConfig = ConfigFactory.parseString(specExecutorJson.get(SerializationConstants.SPEC_EXECUTOR_CONFIG_KEY).getAsString());
      SpecExecutor specExecutor;
      try {
        URI specExecutorUri = new URI(specExecutorConfig.getString(SerializationConstants.SPEC_EXECUTOR_URI_KEY));
        specExecutor = this.topologySpecMap.get(specExecutorUri).getSpecExecutor();
      } catch (Exception e) {
        log.error("Error deserializing specExecutor {}", specExecutorConfig);
        throw new RuntimeException(e);
      }

      JobExecutionPlan jobExecutionPlan = new JobExecutionPlan(jobSpec, specExecutor);
      jobExecutionPlan.setExecutionStatus(executionStatus);

      JsonElement flowStartTime = serializedJobExecutionPlan.get(SerializationConstants.FLOW_START_TIME_KEY);
      if (flowStartTime != null) {
        jobExecutionPlan.setFlowStartTime(flowStartTime.getAsLong());
      }

      try {
        String jobExecutionFuture = serializedJobExecutionPlan.get(SerializationConstants.JOB_EXECUTION_FUTURE).getAsString();
        Future future = specExecutor.getProducer().get().deserializeAddSpecResponse(jobExecutionFuture);
        jobExecutionPlan.setJobFuture(Optional.fromNullable(future));

      } catch (ExecutionException | InterruptedException e) {
        log.warn("Error during deserialization of JobExecutionFuture.");
        throw new RuntimeException(e);
      }

      jobExecutionPlans.add(jobExecutionPlan);
    }
    return jobExecutionPlans;
  }
}
