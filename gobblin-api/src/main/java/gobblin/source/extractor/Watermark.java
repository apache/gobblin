package gobblin.source.extractor;

import gobblin.fork.Copyable;

public interface Watermark extends Comparable<Watermark>, Copyable<Watermark> {

  public void initFromJson(String json);

  public String toJson();

  public float calculatePercentCompletion(Watermark lowWatermark, Watermark expectedHighWatermark);
}
