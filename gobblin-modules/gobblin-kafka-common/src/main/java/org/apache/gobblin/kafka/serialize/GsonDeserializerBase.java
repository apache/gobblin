package org.apache.gobblin.kafka.serialize;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;


public class GsonDeserializerBase<T extends JsonElement> {
  private static final Gson GSON = new Gson();

  public void configure(Map<String, ?> configs, boolean isKey) {
    // Do nothing
  }

  public T deserialize(String topic, byte[] data) {
    return (T) GSON.fromJson(new String(data, StandardCharsets.UTF_8), JsonElement.class);
  }

  public void close() {
    // Do nothing
  }
}
