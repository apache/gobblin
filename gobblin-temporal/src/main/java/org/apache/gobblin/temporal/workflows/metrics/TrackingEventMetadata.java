package org.apache.gobblin.temporal.workflows.metrics;

import java.util.List;

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

import org.apache.gobblin.configuration.State;
import org.apache.gobblin.instrumented.Instrumented;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;
import org.apache.gobblin.metrics.event.EventSubmitter;

import static org.apache.gobblin.instrumented.GobblinMetricsKeys.CLASS_META;


/**
 * Package private impl for sending the core essence of an {@link EventSubmitter} over the wire
 *
 * TODO: Fix this description
 *
 * Serializer for jackson that does its best to serialize a {@link EventSubmitter} from a json representation.
 * NOTE: Due to the nested bidirectional nature of the {@link org.apache.gobblin.metrics.InnerMetricContext} and
 * usage of {@link Gauge} this is not a perfect serialization but it is sufficient for passing important monitoring
 * like original Azkaban flow name and gobblin job id
 */
@Getter
public class TrackingEventMetadata {
  private final List<Tag<?>> tags;
  private final String namespace;
  private final Class callerClass;

  @JsonCreator
  private TrackingEventMetadata(
      @JsonProperty("tags") List<Tag<?>> tags,
      @JsonProperty("namespace") String namespace,
      @JsonProperty("callerClass") Class callerClass) {
    this.tags = tags;
    this.namespace = namespace;
    this.callerClass = callerClass;
  }

  public TrackingEventMetadata(List<Tag<?>> tags, String namespace) {
    // Explicitly send class over the wire to avoid any classloader issues
    this(tags, namespace, tags.stream()
        .filter(tag -> tag.getKey().equals(CLASS_META))
        .findAny()
        .map(tag -> (String) tag.getValue())
        .map(TrackingEventMetadata::resolveClass)
        .orElse(TrackingEventMetadata.class));
  }

  public TrackingEventMetadata(EventSubmitter eventSubmitter) {
    this(eventSubmitter.getTags(), eventSubmitter.getNamespace());
  }

  public EventSubmitter createEventSubmitter() {
    MetricContext metricContext = Instrumented.getMetricContext(new State(), callerClass, tags);
    return new EventSubmitter.Builder(metricContext, namespace).build();
  }

  private static Class resolveClass(String canonicalClassName) {
    try {
      return Class.forName(canonicalClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

  }
}
