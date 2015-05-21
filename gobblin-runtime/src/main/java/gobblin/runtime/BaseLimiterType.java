package gobblin.runtime;

/**
 * An enumeration of types of {@link Limiter}s supported out-of-the-box.
 *
 * @author ynli
 */
public enum BaseLimiterType {

  /**
   * For {@link RateBasedLimiter}.
   */
  RATE_BASED("rate"),

  /**
   * For {@link TimeBasedLimiter}.
   */
  TIME_BASED("time"),

  /**
   * For {@link CountBasedLimiter}.
   */
  COUNT_BASED("count"),

  /**
   * For {@link PoolBasedLimiter}.
   */
  POOL_BASED("pool");

  private final String name;

  BaseLimiterType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return this.name;
  }

  /**
   * Get a {@link BaseLimiterType} for the given name.
   *
   * @param name the given name
   * @return a {@link BaseLimiterType} for the given name
   */
  public static BaseLimiterType forName(String name) {
    if (name.equalsIgnoreCase(RATE_BASED.name)) {
      return RATE_BASED;
    }
    if (name.equalsIgnoreCase(TIME_BASED.name)) {
      return TIME_BASED;
    }
    if (name.equalsIgnoreCase(COUNT_BASED.name)) {
      return COUNT_BASED;
    }
    if (name.equalsIgnoreCase(POOL_BASED.name)) {
      return POOL_BASED;
    }
    throw new IllegalArgumentException("No Limiter implementation available for name: " + name);
  }
}
