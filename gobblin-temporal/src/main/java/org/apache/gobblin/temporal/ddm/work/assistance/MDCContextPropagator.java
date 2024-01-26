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

package org.apache.gobblin.temporal.ddm.work.assistance;

import com.google.protobuf.ByteString;
import io.temporal.api.common.v1.Payload;
import io.temporal.common.context.ContextPropagator;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;


/**
 * Context propagator for MDC in Temporal GaaS flows, which is used to share context between workflows and activities.
 * Keys added to MDC will show up in Temporal headers serialized as protobufs
 */
@Slf4j
public class MDCContextPropagator implements ContextPropagator {

  public String getName() {
    return this.getClass().getName();
  }

  public Object getCurrentContext() {
    return MDC.getCopyOfContextMap();
  }

  public void setCurrentContext(Object context) {
    Map<String, String> contextMap = (Map<String, String>)context;
    for (Map.Entry<String, String> entry : contextMap.entrySet()) {
      MDC.put(entry.getKey(), entry.getValue());
    }
  }

  public Map<String, Payload> serializeContext(Object context) {
    Map<String, String> contextMap = (Map<String, String>)context;
    Map<String, Payload> serializedContext = new HashMap<>();
    for (Map.Entry<String, String> entry : contextMap.entrySet()) {
      Payload payload = Payload.newBuilder().setData(ByteString.copyFrom(entry.getValue().getBytes(StandardCharsets.UTF_8))).build();
      serializedContext.put(entry.getKey(), payload);
    }
    return serializedContext;
  }

  public Object deserializeContext(Map<String, Payload> context) {
    Map<String, String> contextMap = new HashMap<>();
    for (Map.Entry<String, Payload> entry : context.entrySet()) {
      contextMap.put(entry.getKey(), entry.getValue().getData().toString(StandardCharsets.UTF_8));
    }
    return contextMap;
  }
}