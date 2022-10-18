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

package org.apache.gobblin.runtime.spec_serde;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import org.apache.gobblin.runtime.api.JobSpec;


public class JobSpecSerializer implements JsonSerializer<JobSpec> {
  public static final String JOB_SPEC_URI_KEY = "uri";
  public static final String JOB_SPEC_VERSION_KEY = "version";
  public static final String JOB_SPEC_DESCRIPTION_KEY = "description";
  public static final String JOB_SPEC_TEMPLATE_URI_KEY = "templateURI";
  public static final String JOB_SPEC_CONFIG_AS_PROPERTIES_KEY = "configAsProperties";

  @Override
  public JsonElement serialize(JobSpec src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject jobSpecJson = new JsonObject();

    jobSpecJson.add(JOB_SPEC_URI_KEY, context.serialize(src.getUri()));
    jobSpecJson.add(JOB_SPEC_VERSION_KEY, context.serialize(src.getVersion()));
    jobSpecJson.add(JOB_SPEC_DESCRIPTION_KEY, context.serialize(src.getDescription()));
    jobSpecJson.add(JOB_SPEC_TEMPLATE_URI_KEY, src.getTemplateURI().isPresent() ? context.serialize(src.getTemplateURI().get()) : null);
    jobSpecJson.add(JOB_SPEC_CONFIG_AS_PROPERTIES_KEY, context.serialize(src.getConfigAsProperties()));
    // NOTE: do not serialize `JobSpec.jobTemplate`, since `transient`

    return jobSpecJson;
  }
}
