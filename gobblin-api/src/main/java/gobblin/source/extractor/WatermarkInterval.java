package gobblin.source.extractor;

import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Each {@link gobblin.source.workunit.WorkUnit} has a corresponding {@link WatermarkInterval}. The
 * {@link WatermarkInterval} represents the range of the data that needs to be pulled for the {@link WorkUnit}. So, the
 * {@link gobblin.source.workunit.WorkUnit} should pull data from the {@link #lowWatermark} to the
 * {@link #expectedHighWatermark}.
 */
public class WatermarkInterval {

  private Watermark lowWatermark;
  private Watermark expectedHighWatermark;

  private static final String WATERMARK_TO_JSON_CLASS_NAME = "watermark.to.json.class.name";
  private static final String WATERMARK_TO_JSON_VALUE = "watermark.to.json.value";
  private static final Integer WATERMARK_TO_JSON_ARRAY_SIZE = 2;

  // Needed for the Writable interface
  public WatermarkInterval() {
  }

  private WatermarkInterval(Watermark lowWatermark, Watermark expectedHighWatermark) {
    this.lowWatermark = lowWatermark;
    this.expectedHighWatermark = expectedHighWatermark;
  }

  public static class Builder {

    private Watermark lowWatermark;
    private Watermark expectedHighWatermark;

    public Builder withLowWatermark(Watermark lowWatermark) {
      this.lowWatermark = lowWatermark;
      return this;
    }

    public Builder withExpectedHighWatermark(Watermark expectedHighWatermark) {
      this.expectedHighWatermark = expectedHighWatermark;
      return this;
    }

    public WatermarkInterval build() {
      Preconditions.checkNotNull(this.lowWatermark, "Must specify a low watermark");
      Preconditions.checkNotNull(this.expectedHighWatermark, "Must specify an expected high watermark");

      return new WatermarkInterval(this.lowWatermark, this.expectedHighWatermark);
    }
  }

  public Watermark getLowWatermark() {
    return this.lowWatermark;
  }

  public Watermark getExpectedHighWatermark() {
    return this.expectedHighWatermark;
  }

  public JsonElement toJson() {
    JsonArray jsonArray = new JsonArray();

    JsonObject lowWatermarkJson = watermarkToJsonObject(this.lowWatermark);
    JsonObject expectedHighWatermarkJson = watermarkToJsonObject(this.expectedHighWatermark);

    jsonArray.add(lowWatermarkJson);
    jsonArray.add(expectedHighWatermarkJson);

    return jsonArray;
  }

  public void fromJson(JsonElement json) {
    Preconditions.checkArgument(json.isJsonArray(), "The given " + json.getClass().getName() + " must a " + JsonArray.class.getName());

    JsonArray jsonArray = json.getAsJsonArray();
    Preconditions.checkArgument(jsonArray.size() == WATERMARK_TO_JSON_ARRAY_SIZE, "The given " + JsonArray.class.getName() + " must be of size " + WATERMARK_TO_JSON_ARRAY_SIZE);

    Iterator<JsonElement> jsonElements = jsonArray.iterator();
    JsonElement lowWatermarkJsonElement = jsonElements.next();
    JsonElement expectedHighWatermarkJsonElement = jsonElements.next();

    Preconditions.checkArgument(
        lowWatermarkJsonElement.isJsonObject() && expectedHighWatermarkJsonElement.isJsonObject(),
        "All elements of in the given " + JsonArray.class.getName() + " must of type " + JsonObject.class.getName());

    try {
      this.lowWatermark = jsonObjectToWatermark(lowWatermarkJsonElement.getAsJsonObject());
    } catch (InstantiationException e) {
      throw new RuntimeException("Could not create the low watermark", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not create the low watermark", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not create the low watermark", e);
    }

    try {
      this.expectedHighWatermark = jsonObjectToWatermark(expectedHighWatermarkJsonElement.getAsJsonObject());
    } catch (InstantiationException e) {
      throw new RuntimeException("Could not create the expected high watermark", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Could not create the expected high watermark", e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Could not create the expected high watermark", e);
    }
  }

  private JsonObject watermarkToJsonObject(Watermark watermark) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(WATERMARK_TO_JSON_CLASS_NAME, watermark.getClass().getName());
    jsonObject.add(WATERMARK_TO_JSON_VALUE, watermark.toJson());
    return jsonObject;
  }

  private Watermark jsonObjectToWatermark(JsonObject jsonObject) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
   Watermark watermark = (Watermark) Class.forName(jsonObject.get(WATERMARK_TO_JSON_CLASS_NAME).getAsString()).newInstance();
   watermark.fromJson(jsonObject.get(WATERMARK_TO_JSON_VALUE));
   return watermark;
  }
}
