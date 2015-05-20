package gobblin.source.extractor;

import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * Implementation of {@link Watermark} used for testing purposes in {@link TestWatermark}.
 */
public class TestWatermark implements Watermark {

  private static final Gson GSON = new Gson();

  private long watermark = -1;

  @Override
  public int compareTo(Watermark watermark) {
    TestWatermark testWatermark = GSON.fromJson(watermark.toJson(), this.getClass());
    return Longs.compare(this.watermark, testWatermark.getLongWatermark());
  }

  @Override
  public JsonElement toJson() {
    return WatermarkSerializerHelper.convertWatermarkToJson(this);
  }

  @Override
  public short calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark) {
    TestWatermark testLowWatermark = GSON.fromJson(lowWatermark.toJson(), TestWatermark.class);
    TestWatermark testHighWatermark = GSON.fromJson(highWatermark.toJson(), TestWatermark.class);
    return (short) (100 * (this.watermark - testLowWatermark.getLongWatermark()) / (testHighWatermark
        .getLongWatermark() - testLowWatermark.getLongWatermark()));
  }

  public void setLongWatermark(long watermark) {
    this.watermark = watermark;
  }

  public long getLongWatermark() {
    return this.watermark;
  }
}
