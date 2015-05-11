package gobblin.source.extractor;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * Provides default implementation for converting a {@link Watermark} to a {@link JsonElement}. The class uses
 * {@link Gson#toJsonTree(Object)} method. Check out <a href="https://code.google.com/p/google-gson/">GSON</a> for more
 * information.
 *
 * TODO - once Gobblin migrates to Java 8 this method can become the default implementation of {@link Watermark#toJson()}.
 */
public class DefaultWatermarkSerializer {

  private static final Gson GSON = new Gson();

  public JsonElement convertWatermarkToJson(Watermark watermark) {
    return GSON.toJsonTree(watermark);
  }
}
