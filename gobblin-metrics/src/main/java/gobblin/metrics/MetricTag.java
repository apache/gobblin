package gobblin.metrics;

/**
 * A class representing a dimension or property associated with a {@link MetricContext}.
 *
 * @author ynli
 */
public class MetricTag {

  /**
   * Enumeration of supported types of {@link MetricTag}s.
   */
  public enum TagType {
    // A type for tags whose values are known before runtime
    STATIC,
    // A type for tags whose values are known at runtime
    RUNTIME
  }

  private final String name;
  private final TagType type;

  public MetricTag(String name, TagType type) {
    this.name = name;
    this.type = type;
  }

  /**
   * Get the name of this {@link MetricTag}.
   *
   * @return the name of this {@link MetricTag}
   */
  public String getName() {
    return this.name;
  }

  /**
   * Get the {@link TagType} of this {@link MetricTag}.
   *
   * @return the {@link TagType} of this {@link MetricTag}
   */
  public TagType getType() {
    return this.type;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof MetricTag)) {
      return false;
    }

    MetricTag otherTag = (MetricTag) other;
    return this.name.equals(otherTag.name) && this.type.equals(otherTag.type);
  }

  @Override
  public String toString() {
    return this.name;
  }
}
