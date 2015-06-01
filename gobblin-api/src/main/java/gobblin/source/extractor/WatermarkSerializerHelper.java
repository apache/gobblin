package gobblin.source.extractor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * Provides default implementation for converting a {@link Watermark} to a {@link JsonElement}, and vice versa. The
 * class uses <a href="https://code.google.com/p/google-gson/">GSON</a> to achieve this. This class provides a default
 * way to serialize and de-serialize {@link Watermark}s, and is useful for implementing the {@link Watermark#toJson()}
 * method.
 */
public class WatermarkSerializerHelper {

  private static final Gson GSON = new Gson();

  /**
   * Converts a {@link Watermark} to a {@link JsonElement} using the {@link Gson#toJsonTree(Object)} method.
   *
   * @param watermark the {@link Watermark} that needs to be converted to json.
   * @return a {@link JsonElement} that represents the given {@link Watermark}.
   */
  public static JsonElement convertWatermarkToJson(Watermark watermark) {
    return GSON.toJsonTree(watermark);
  }

  /**
   * Converts a {@link JsonElement} into the specified class type using the {@link Gson#fromJson(JsonElement, Class)}
   * method.
   *
   * @param jsonElement is a {@link JsonElement} that will be converted into a {@link Watermark}.
   * @param clazz is the {@link Class} that the {@link JsonElement} will be converted into.
   * @return an instance of a class that extends {@link Watermark}.
   */
  public static <T extends Watermark> T convertJsonToWatermark(JsonElement jsonElement, Class<T> clazz) {
    return GSON.fromJson(jsonElement, clazz);
  }
}
