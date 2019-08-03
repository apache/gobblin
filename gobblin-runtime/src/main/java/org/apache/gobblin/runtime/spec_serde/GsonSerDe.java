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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;


/**
 * SerDe library used in GaaS for {@link org.apache.gobblin.runtime.api.SpecStore} and
 * {@link org.apache.gobblin.service.modules.orchestration.DagStateStore}.
 *
 * The solution is built on top of {@link Gson} library.
 * @param <T> The type of object to be serialized.
 */
public class GsonSerDe<T> {
  private final Gson gson;
  private final JsonSerializer<T> serializer;
  private final JsonDeserializer<T> deserializer;
  private final Type type;

  public GsonSerDe(Type type, JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
    this.type = type;

    this.gson = new GsonBuilder().registerTypeAdapter(type, serializer)
        .registerTypeAdapter(type, deserializer)
        .create();
  }

  public String serialize(T object) {
    return gson.toJson(object, type);
  }

  public T deserialize(String serializedObject) {
    return gson.fromJson(serializedObject, type);
  }
}