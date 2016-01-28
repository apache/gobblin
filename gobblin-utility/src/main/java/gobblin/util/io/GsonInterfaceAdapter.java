/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.util.io;

import java.lang.reflect.Type;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;


/**
 * A {@link Gson} interface adapter that makes it possible to serialize and deserialize an object
 * with an interface or abstract field with {@link Gson}.
 *
 * <p>
 *    Suppose a class <pre>MyClass</pre> contains an interface field <pre>MyInterface field</pre>. To serialize
 *    and deserialize a <pre>MyClass</pre> object, do the following:
 *
 *    <pre>
 *          {@code
 *            MyClass object = new MyClass();
 *            Gson gson = GsonInterfaceAdapter.getGson(MyInterface.class);
 *            String json = gson.toJson(object);
 *            Myclass object2 = gson.fromJson(json, MyClass.class);
 *          }
 *    </pre>
 * </p>
 *
 * @author ziliu
 *
 * @param <T> The interface or abstract type to be serialized and deserialized with {@link Gson}.
 */
public class GsonInterfaceAdapter<T> implements JsonSerializer<T>, JsonDeserializer<T> {

  private static final String OBJECT_TYPE = "object-type";
  private static final String OBJECT_DATA = "object-data";

  @Override
  public JsonElement serialize(T object, Type interfaceType, JsonSerializationContext context) {
    JsonObject wrapper = new JsonObject();
    wrapper.addProperty(OBJECT_TYPE, object.getClass().getName());
    wrapper.add(OBJECT_DATA, context.serialize(object));
    return wrapper;
  }

  @Override
  public T deserialize(JsonElement elem, Type interfaceType, JsonDeserializationContext context)
      throws JsonParseException {
    JsonObject wrapper = (JsonObject) elem;
    JsonElement typeName = get(wrapper, OBJECT_TYPE);
    JsonElement data = get(wrapper, OBJECT_DATA);
    Type actualType = typeForName(typeName);
    return context.deserialize(data, actualType);
  }

  private Type typeForName(JsonElement typeElem) {
    try {
      return Class.forName(typeElem.getAsString());
    } catch (ClassNotFoundException e) {
      throw new JsonParseException(e);
    }
  }

  private JsonElement get(JsonObject wrapper, String memberName) {
    JsonElement elem = wrapper.get(memberName);
    Preconditions.checkNotNull(elem);
    return elem;
  }

  public static <T> Gson getGson(Class<T> clazz) {
    return new GsonBuilder().registerTypeAdapter(clazz, new GsonInterfaceAdapter<T>()).create();
  }
}
