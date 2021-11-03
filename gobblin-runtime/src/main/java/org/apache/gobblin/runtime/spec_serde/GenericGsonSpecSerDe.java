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

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializer;
import org.apache.gobblin.runtime.api.Spec;
import org.apache.gobblin.runtime.api.SpecSerDe;
import org.apache.gobblin.runtime.api.SpecSerDeException;


/**
 * Abstract {@link SpecSerDe} providing scaffolding to serialize a specific {@link Spec} derived class as JSON using {@link Gson}.
 * Extending classes MUST supply a per-instance serializer and a deserializer and SHOULD be concrete (non-generic), to permit
 * naming/reference from properties-based configuration.
 */
public abstract class GenericGsonSpecSerDe<T extends Spec> implements SpecSerDe {
  private final GsonSerDe<T> gsonSerDe;
  private final Class<T> specClass;

  protected GenericGsonSpecSerDe(Class<T> specClass, JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
    this.specClass = specClass;
    this.gsonSerDe = new GsonSerDe<T>(specClass, serializer, deserializer);
  }

  @Override
  public byte[] serialize(Spec spec) throws SpecSerDeException {
    if (!specClass.isInstance(spec)) {
      throw new SpecSerDeException("Failed to serialize spec " + spec.getUri() + ", only " + specClass.getName() + " is supported");
    }

    try {
      return this.gsonSerDe.serialize(specClass.cast(spec)).getBytes(Charsets.UTF_8);
    } catch (JsonParseException e) {
      throw new SpecSerDeException(spec, e);
    }
  }

  @Override
  public Spec deserialize(byte[] spec) throws SpecSerDeException {
    try {
      return this.gsonSerDe.deserialize(new String(spec, Charsets.UTF_8));
    } catch (JsonParseException e) {
      throw new SpecSerDeException(e);
    }
  }
}
