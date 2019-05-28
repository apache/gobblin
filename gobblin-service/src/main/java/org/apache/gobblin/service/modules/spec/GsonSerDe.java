package org.apache.gobblin.service.modules.spec;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;


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
  private final Type typeToken;

  public GsonSerDe(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;

    typeToken = new TypeToken<T>() {
    }.getType();
    this.gson = new GsonBuilder().registerTypeAdapter(typeToken, serializer)
        .registerTypeAdapter(typeToken, deserializer)
        .create();
  }

  public String serialize(T object) {
    return gson.toJson(object, typeToken);
  }

  public T deserialize(String serializedObject) {
    return gson.fromJson(serializedObject, typeToken);
  }
}
