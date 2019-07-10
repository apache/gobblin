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
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;

import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.runtime.api.JobSpec;

@Slf4j
public class JobExecutionPlanListSerializer implements JsonSerializer<List<JobExecutionPlan>> {
  /**
   * Gson invokes this call-back method during serialization when it encounters a field of the
   * specified type.
   *
   * <p>In the implementation of this call-back method, you should consider invoking
   * {@link JsonSerializationContext#serialize(Object, Type)} method to create JsonElements for any
   * non-trivial field of the {@code src} object. However, you should never invoke it on the
   * {@code src} object itself since that will cause an infinite loop (Gson will call your
   * call-back method again).</p>
   *
   * @param src the object that needs to be converted to Json.
   * @param typeOfSrc the actual type (fully genericized version) of the source object.
   * @param context
   * @return a JsonElement corresponding to the specified object.
   */
  @Override
  public JsonElement serialize(List<JobExecutionPlan> src, Type typeOfSrc, JsonSerializationContext context) {
    JsonArray jsonArray = new JsonArray();

    for (JobExecutionPlan jobExecutionPlan: src) {
      JsonObject jobExecutionPlanJson = new JsonObject();
      JsonObject jobSpecJson = new JsonObject();
      JobSpec jobSpec = jobExecutionPlan.getJobSpec();
      String uri = (jobSpec.getUri() != null) ? jobSpec.getUri().toString() : null;
      jobSpecJson.addProperty(SerializationConstants.JOB_SPEC_URI_KEY, uri);
      jobSpecJson.addProperty(SerializationConstants.JOB_SPEC_VERSION_KEY, jobSpec.getVersion());
      jobSpecJson.addProperty(SerializationConstants.JOB_SPEC_DESCRIPTION_KEY, jobSpec.getDescription());
      String jobSpecTemplateURI = (jobSpec.getTemplateURI().isPresent()) ? jobSpec.getTemplateURI().get().toString() : null;
      jobSpecJson.addProperty(SerializationConstants.JOB_SPEC_TEMPLATE_URI_KEY, jobSpecTemplateURI);
      jobSpecJson.addProperty(SerializationConstants.JOB_SPEC_CONFIG_KEY, jobSpec.getConfig().root().render(ConfigRenderOptions.concise()));
      jobExecutionPlanJson.add(SerializationConstants.JOB_SPEC_KEY, jobSpecJson);
      Config specExecutorConfig;
      try {
         specExecutorConfig = jobExecutionPlan.getSpecExecutor().getConfig().get();
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error serializing JobExecutionPlan {}", jobExecutionPlan.toString());
        throw new RuntimeException(e);
      }
      JsonObject specExecutorJson = new JsonObject();
      specExecutorJson.addProperty(SerializationConstants.SPEC_EXECUTOR_CONFIG_KEY,
          specExecutorConfig.root().render(ConfigRenderOptions.concise()));
      specExecutorJson.addProperty(SerializationConstants.SPEC_EXECUTOR_CLASS_KEY,
          jobExecutionPlan.getSpecExecutor().getClass().getName());
      jobExecutionPlanJson.add(SerializationConstants.SPEC_EXECUTOR_KEY, specExecutorJson);

      String executionStatus = jobExecutionPlan.getExecutionStatus().name();
      jobExecutionPlanJson.addProperty(SerializationConstants.EXECUTION_STATUS_KEY, executionStatus);

      try {
        String jobExecutionFuture = jobExecutionPlan.getSpecExecutor().getProducer().get()
            .serializeAddSpecResponse(jobExecutionPlan.getJobFuture().orNull());
        jobExecutionPlanJson.addProperty(SerializationConstants.JOB_EXECUTION_FUTURE, jobExecutionFuture);
      } catch (InterruptedException | ExecutionException e) {
        log.warn("Error during serialization of JobExecutionFuture.");
        throw new RuntimeException(e);
      }

      jsonArray.add(jobExecutionPlanJson);
    }
    return jsonArray;
  }
}
