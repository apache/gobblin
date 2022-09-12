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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import org.apache.gobblin.runtime.api.JobSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobSpecDeserializer implements JsonDeserializer<JobSpec> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobSpecDeserializer.class);

  @Override
  public JobSpec deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
    JsonObject jsonObject = json.getAsJsonObject();

    String uri = jsonObject.get(JobSpecSerializer.JOB_SPEC_URI_KEY).getAsString();
    String version = jsonObject.get(JobSpecSerializer.JOB_SPEC_VERSION_KEY).getAsString();
    String description = jsonObject.get(JobSpecSerializer.JOB_SPEC_DESCRIPTION_KEY).getAsString();
    Optional<URI> templateURI = Optional.ofNullable(jsonObject.get(JobSpecSerializer.JOB_SPEC_TEMPLATE_URI_KEY)).flatMap(jsonElem -> {
      try {
        return Optional.of(new URI(jsonElem.getAsString()));
      } catch (URISyntaxException | RuntimeException e) {
        LOGGER.warn(String.format("error deserializing '%s' as a URI: %s", jsonElem.toString(), e.getMessage()));
        return Optional.empty();
      }
    });
    Properties properties;

    try {
      properties = context.deserialize(jsonObject.get(JobSpecSerializer.JOB_SPEC_CONFIG_AS_PROPERTIES_KEY), Properties.class);
    } catch (JsonParseException e) {
      // for backward compatibility... (is this needed for `JobSpec`, or only for (inspiration) `FlowSpec`???)
      properties = new Properties();
      try {
        properties.load(new StringReader(jsonObject.get(JobSpecSerializer.JOB_SPEC_CONFIG_AS_PROPERTIES_KEY).getAsString()));
      } catch (IOException ioe) {
        throw new JsonParseException(e);
      }
    }

    JobSpec.Builder builder = JobSpec.builder(uri).withVersion(version).withDescription(description).withConfigAsProperties(properties);
    if (templateURI.isPresent()) {
      builder = builder.withTemplate(templateURI.get());
    }

    return builder.build();
  }
}
