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

package org.apache.gobblin.dataset;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import org.apache.gobblin.util.io.GsonInterfaceAdapter;


/**
 * A descriptor is a simplified representation of a resource, which could be a dataset, dataset partition, file, etc.
 * It is a digest or abstract, which contains identification properties of the actual resource object, such as ID, name
 * primary keys, version, etc
 *
 * <p>
 *   When the original object has complicated inner structure and there is a requirement to send it over the wire,
 *   it's a time to define a corresponding {@link Descriptor} becomes. In this case, the {@link Descriptor} can just
 *   have minimal information enough to construct the original object on the other side of the wire
 * </p>
 *
 * <p>
 *   When the cost to instantiate the complete original object is high, for example, network calls are required, but
 *   the use cases are limited to the identification properties, define a corresponding {@link Descriptor} becomes
 *   handy
 * </p>
 */
@RequiredArgsConstructor
public class Descriptor {

  /** Use gson for ser/de */
  public static final Gson GSON =
      new GsonBuilder().registerTypeAdapterFactory(new GsonInterfaceAdapter(Descriptor.class)).create();
  /** Type token for ser/de descriptor list */
  private static final Type DESCRIPTOR_LIST_TYPE = new TypeToken<ArrayList<Descriptor>>(){}.getType();

  /** Name of the resource */
  @Getter
  private final String name;

  @Override
  public String toString() {
    return GSON.toJson(this);
  }

  public Descriptor copy() {
    return new Descriptor(name);
  }

  /**
   * Serialize any {@link Descriptor} object as json string
   *
   * <p>
   *   Note: it can serialize subclasses
   * </p>
   */
  public static String toJson(Descriptor descriptor) {
    return GSON.toJson(descriptor);
  }

  /**
   * Deserialize the json string to the a {@link Descriptor} object
   */
  public static Descriptor fromJson(String json) {
    return fromJson(json, Descriptor.class);
  }

  /**
   * Deserialize the json string to the specified {@link Descriptor} object
   */
  public static <T extends Descriptor> T fromJson(String json, Class<T> clazz) {
    return GSON.fromJson(json, clazz);
  }

  /**
   * Serialize a list of descriptors as json string
   */
  public static String toJson(List<Descriptor> descriptors) {
    return GSON.toJson(descriptors, DESCRIPTOR_LIST_TYPE);
  }

  /**
   * Deserialize the string, resulted from {@link #toJson(List)}, to a list of descriptors
   */
  public static List<Descriptor> fromJsonList(String jsonList) {
    return GSON.fromJson(jsonList, DESCRIPTOR_LIST_TYPE);
  }
}
