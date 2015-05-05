package gobblin.source.extractor;

import gobblin.fork.Copyable;

/**
 * A {@link Watermark} represents a checkpoint in data processing, and indicates that all data up to  specific point has
 * been pulled from some source. Implementations of this interface are responsible for defining data structures in order
 * to track this state.
 *
 * <p>
 *  A {@link Watermark} will be serialized in {@link gobblin.source.workunit.WorkUnit}s and
 *  {@link gobblin.configuration.WorkUnitState}s. The {@link #toJson()} method will be used to serialize the
 *  {@link Watermark}, and the {@link #initFromJson(String)| method will be responsible for initializing the
 *  {@link Watermark} given the output of {@link #toJson()}.
 * </p>
 */
public interface Watermark extends Comparable<Watermark>, Copyable<Watermark> {

  public void initFromJson(String json);

  public String toJson();

  /**
   * This method must return a value from [0, 1]. The value should correspond to a percent completion. Given two
   * {@link Watermark} values, where the lowWatermark is the starting point, and the highWatermark is the goal, what
   * is the percent completion of this {@link Watermark}.
   */
  public float calculatePercentCompletion(Watermark lowWatermark, Watermark highWatermark);
}
