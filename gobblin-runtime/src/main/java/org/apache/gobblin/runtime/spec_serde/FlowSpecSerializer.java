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

import java.lang.reflect.Type;
import java.net.URI;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.typesafe.config.ConfigRenderOptions;

import org.apache.gobblin.runtime.api.FlowSpec;
import org.apache.gobblin.runtime.api.Spec;


public class FlowSpecSerializer implements JsonSerializer<FlowSpec> {
  public static final String FLOW_SPEC_URI_KEY= "uri";
  public static final String FLOW_SPEC_VERSION_KEY = "version";
  public static final String FLOW_SPEC_DESCRIPTION_KEY = "description";
  public static final String FLOW_SPEC_CONFIG_KEY = "config";
  public static final String FLOW_SPEC_CONFIG_AS_PROPERTIES_KEY = "configAsProperties";
  public static final String FLOW_SPEC_TEMPLATE_URIS_KEY = "templateURIs";
  public static final String FLOW_SPEC_CHILD_SPECS_KEY = "childSpecs";

  @Override
  public JsonElement serialize(FlowSpec src, Type typeOfSrc, JsonSerializationContext context) {
    JsonObject flowSpecJson = new JsonObject();

    flowSpecJson.add(FLOW_SPEC_URI_KEY, context.serialize(src.getUri()));
    flowSpecJson.add(FLOW_SPEC_VERSION_KEY, context.serialize(src.getVersion()));
    flowSpecJson.add(FLOW_SPEC_DESCRIPTION_KEY, context.serialize(src.getDescription()));
    flowSpecJson.add(FLOW_SPEC_CONFIG_KEY, context.serialize(src.getConfig().root().render(ConfigRenderOptions.concise())));

    flowSpecJson.add(FLOW_SPEC_CONFIG_AS_PROPERTIES_KEY, context.serialize(src.getConfigAsProperties()));

    JsonArray templateURIs = new JsonArray();
    if (src.getTemplateURIs().isPresent()) {
      for (URI templateURI : src.getTemplateURIs().get()) {
        templateURIs.add(context.serialize(templateURI));
      }
    }
    flowSpecJson.add(FLOW_SPEC_TEMPLATE_URIS_KEY, templateURIs);

    JsonArray childSpecs = new JsonArray();
    if (src.getChildSpecs().isPresent()) {
      for (Spec spec : src.getChildSpecs().get()) {
        childSpecs.add(context.serialize(spec));
      }
    }
    flowSpecJson.add(FLOW_SPEC_CHILD_SPECS_KEY, childSpecs);

    return flowSpecJson;
  }
}
