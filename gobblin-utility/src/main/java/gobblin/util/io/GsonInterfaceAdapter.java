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

package gobblin.util.io;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.ClassUtils;

import com.google.common.base.Optional;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;


/**
 * A {@link Gson} interface adapter that makes it possible to serialize and deserialize polymorphic objects.
 *
 * <p>
 *   This adapter will capture all instances of {@link #baseClass} and write them as
 *   {"object-type":"class.name", "object-data":"data"}, allowing for correct serialization and deserialization of
 *   polymorphic objects. The following types will not be captured by the adapter (i.e. they will be written by the
 *   default GSON writer):
 *   - Primitives and boxed primitives
 *   - Arrays
 *   - Collections
 *   - Maps
 *   Additionally, generic classes (e.g. class MyClass<T>) cannot be correctly decoded.
 * </p>
 *
 * <p>
 *   To use:
 *    <pre>
 *          {@code
 *            MyClass object = new MyClass();
 *            Gson gson = GsonInterfaceAdapter.getGson(MyBaseClass.class);
 *            String json = gson.toJson(object);
 *            Myclass object2 = gson.fromJson(json, MyClass.class);
 *          }
 *    </pre>
 * </p>
 *
 * <p>
 *   Note: a useful case is GsonInterfaceAdapter.getGson(Object.class), which will correctly serialize / deserialize
 *   all types except for java generics.
 * </p>
 *
 * @param <T> The interface or abstract type to be serialized and deserialized with {@link Gson}.
 */
@RequiredArgsConstructor
public class GsonInterfaceAdapter implements TypeAdapterFactory {

  protected static final String OBJECT_TYPE = "object-type";
  protected static final String OBJECT_DATA = "object-data";

  private final Class<?> baseClass;

  @Override
  public <R> TypeAdapter<R> create(Gson gson, TypeToken<R> type) {
    if (ClassUtils.isPrimitiveOrWrapper(type.getRawType()) || type.getType() instanceof GenericArrayType
        || CharSequence.class.isAssignableFrom(type.getRawType())
        || (type.getType() instanceof ParameterizedType && (Collection.class.isAssignableFrom(type.getRawType())
            || Map.class.isAssignableFrom(type.getRawType())))) {
      // delegate primitives, arrays, collections, and maps
      return null;
    }
    if (!this.baseClass.isAssignableFrom(type.getRawType())) {
      // delegate anything not assignable from base class
      return null;
    }
    TypeAdapter<R> adapter = new InterfaceAdapter<>(gson, this, type);
    return adapter;
  }

  @AllArgsConstructor
  private static class InterfaceAdapter<R> extends TypeAdapter<R> {

    private final Gson gson;
    private final TypeAdapterFactory factory;
    private final TypeToken<R> typeToken;

    @Override
    public void write(JsonWriter out, R value) throws IOException {
      if (Optional.class.isAssignableFrom(this.typeToken.getRawType())) {
        Optional opt = (Optional) value;
        if (opt != null && opt.isPresent()) {
          Object actualValue = opt.get();
          writeObject(actualValue, out);
        } else {
          out.beginObject();
          out.endObject();
        }
      } else {
        writeObject(value, out);
      }
    }

    @Override
    public R read(JsonReader in) throws IOException {
      JsonElement element = Streams.parse(in);
      if (element.isJsonNull()) {
        return readNull();
      }
      JsonObject jsonObject = element.getAsJsonObject();

      if (this.typeToken.getRawType() == Optional.class) {
        if (jsonObject.has(OBJECT_TYPE)) {
          return (R) Optional.of(readValue(jsonObject, null));
        } else if (jsonObject.entrySet().isEmpty()) {
          return (R) Optional.absent();
        } else {
          throw new IOException("No class found for Optional value.");
        }
      }
      return this.readValue(jsonObject, this.typeToken);
    }

    private <S> S readNull() {
      if (this.typeToken.getRawType() == Optional.class) {
        return (S) Optional.absent();
      }
      return null;
    }

    private <S> void writeObject(S value, JsonWriter out) throws IOException {
      if (value != null) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add(OBJECT_TYPE, new JsonPrimitive(value.getClass().getName()));
        TypeAdapter<S> delegate =
            (TypeAdapter<S>) this.gson.getDelegateAdapter(this.factory, TypeToken.get(value.getClass()));
        jsonObject.add(OBJECT_DATA, delegate.toJsonTree(value));
        Streams.write(jsonObject, out);
      } else {
        out.nullValue();
      }
    }

    private <S> S readValue(JsonObject jsonObject, TypeToken<S> defaultTypeToken) throws IOException {
      try {
        TypeToken<S> actualTypeToken;
        if (jsonObject.isJsonNull()) {
          return null;
        } else if (jsonObject.has(OBJECT_TYPE)) {
          String className = jsonObject.get(OBJECT_TYPE).getAsString();
          Class<S> klazz = (Class<S>) Class.forName(className);
          actualTypeToken = TypeToken.get(klazz);
        } else if (defaultTypeToken != null) {
          actualTypeToken = defaultTypeToken;
        } else {
          throw new IOException("Could not determine TypeToken.");
        }
        TypeAdapter<S> delegate = this.gson.getDelegateAdapter(this.factory, actualTypeToken);
        S value = delegate.fromJsonTree(jsonObject.get(OBJECT_DATA));
        return value;
      } catch (ClassNotFoundException cnfe) {
        throw new IOException(cnfe);
      }
    }

  }

  public static <T> Gson getGson(Class<T> clazz) {
    Gson gson = new GsonBuilder().registerTypeAdapterFactory(new GsonInterfaceAdapter(clazz)).create();
    return gson;
  }
}
