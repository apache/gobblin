package org.apache.gobblin.metrics.event;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

import lombok.Getter;
import lombok.Setter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;

/**
 * The builder builds builds a specific {@link GobblinTrackingEvent} whose metadata has
 * {@value GobblinEventBuilder#EVENT_TYPE} to be {@value #COUNT_EVENT_TYPE}
 *
 * <p>
 * Note: A {@link CountEventBuilder} instance is not reusable
 */
public class CountEventBuilder extends GobblinEventBuilder {

  private static final String COUNT_EVENT_TYPE = "CountEvent";
  private static final String COUNT_KEY = "Count";
  @Setter
  @Getter
  private int count;

  public CountEventBuilder(String name) {
    this(name, NAMESPACE);
  }

  public CountEventBuilder(String name, String namespace) {
    super(name, namespace);
    this.metadata.put(EVENT_TYPE, COUNT_EVENT_TYPE);
  }

  /**
   *
   * @return {@link GobblinTrackingEvent}
   */
  @Override
  public GobblinTrackingEvent build() {
    this.metadata.put(COUNT_KEY, Integer.toString(count));
    return super.build();
  }

  /**
   * Check if the given {@link GobblinTrackingEvent} is a {@link CountEventBuilder}
   */
  public static boolean isCountEvent(GobblinTrackingEvent event) {
    String eventType = event.getMetadata().get(EVENT_TYPE);
    return StringUtils.isNotEmpty(eventType) && eventType.equals(COUNT_EVENT_TYPE);
  }
  /**
   * Create a {@link CountEventBuilder} from a {@link GobblinTrackingEvent}. An inverse function
   * to {@link CountEventBuilder#build()}
   */
  public static CountEventBuilder fromEvent(GobblinTrackingEvent event) {
    if(!isCountEvent(event)) {
      return null;
    }

    Map<String, String> metadata = event.getMetadata();
    CountEventBuilder countEventBuilder = new CountEventBuilder(event.getName());
    metadata.forEach((key, value) -> {
      switch (key) {
        case COUNT_KEY:
          countEventBuilder.setCount(Integer.parseInt(value));
          break;
        default:
          countEventBuilder.addMetadata(key, value);
          break;
      }
    });

    return countEventBuilder;
  }
}
