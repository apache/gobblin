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

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;


public class FlowSpecDeserializer implements JsonDeserializer<FlowSpec> {
  @Override
  public FlowSpec deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
    JsonObject jsonObject = json.getAsJsonObject();

    String uri = jsonObject.get(FlowSpecSerializer.FLOW_SPEC_URI_KEY).getAsString();
    String version = jsonObject.get(FlowSpecSerializer.FLOW_SPEC_VERSION_KEY).getAsString();
    String description = jsonObject.get(FlowSpecSerializer.FLOW_SPEC_DESCRIPTION_KEY).getAsString();
    Config config = ConfigFactory.parseString(jsonObject.get(FlowSpecSerializer.FLOW_SPEC_CONFIG_KEY).getAsString());

    Properties properties;

    try {
      properties = context.deserialize(jsonObject.get(FlowSpecSerializer.FLOW_SPEC_CONFIG_AS_PROPERTIES_KEY), Properties.class);
    } catch (JsonParseException e) {
      // for backward compatibility
      properties = new Properties();
      try {
        properties.load(new StringReader(jsonObject.get(FlowSpecSerializer.FLOW_SPEC_CONFIG_AS_PROPERTIES_KEY).getAsString()));
      } catch (IOException ioe) {
        throw new JsonParseException(e);
      }
    }

    Set<URI> templateURIs = new HashSet<>();
    try {
      for (JsonElement template : jsonObject.get(FlowSpecSerializer.FLOW_SPEC_TEMPLATE_URIS_KEY).getAsJsonArray()) {
        templateURIs.add(new URI(template.getAsString()));
      }
    } catch (URISyntaxException e) {
      throw new JsonParseException(e);
    }

    List<Spec> childSpecs = new ArrayList<>();
    for (JsonElement spec : jsonObject.get(FlowSpecSerializer.FLOW_SPEC_CHILD_SPECS_KEY).getAsJsonArray()) {
      childSpecs.add(context.deserialize(spec, FlowSpec.class));
    }

    FlowSpec.Builder builder = FlowSpec.builder(uri).withVersion(version).withDescription(description).withConfig(config)
        .withConfigAsProperties(properties);
    if (!templateURIs.isEmpty()) {
      builder = builder.withTemplates(templateURIs);
    }
    if (!childSpecs.isEmpty()) {
      builder = builder.withChildSpecs(childSpecs);
    }

    return builder.build();
  }
}
