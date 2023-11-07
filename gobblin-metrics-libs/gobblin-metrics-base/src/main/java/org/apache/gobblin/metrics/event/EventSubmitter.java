/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gobblin.metrics.event;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.codahale.metrics.Gauge;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import lombok.Getter;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;
import org.apache.gobblin.metrics.Tag;


/**
 * Wrapper around Avro {@link org.apache.gobblin.metrics.GobblinTrackingEvent.Builder} simplifying handling {@link org.apache.gobblin.metrics.GobblinTrackingEvent}s.
 *
 * <p>
 *   Instances of this class are immutable. Calling set* methods returns a copy of the calling instance.
 * </p>
 *
 */
@JsonSerialize(using = EventSubmitter.Serializer.class)
@JsonDeserialize(using = EventSubmitter.Deserializer.class)
public class EventSubmitter {

  public static final String EVENT_TYPE = "eventType";

  private final Map<String, String> metadata;
  @Getter
  private final String namespace;
  @Getter
  private final Optional<MetricContext> metricContext;

  public static class Builder {
    private final Optional<MetricContext> metricContext;
    private final Map<String, String> metadata;
    private final String namespace;

    public Builder(MetricContext metricContext, String namespace) {
      this(Optional.fromNullable(metricContext), namespace);
    }

    public Builder(Optional<MetricContext> metricContext, String namespace) {
      this.metricContext = metricContext;
      this.namespace = namespace;
      this.metadata = Maps.newHashMap();
    }

    public Builder addMetadata(String key, String value) {
      this.metadata.put(key, value);
      return this;
    }

    public Builder addMetadata(Map<? extends String, ? extends String> additionalMetadata) {
      this.metadata.putAll(additionalMetadata);
      return this;
    }

    public EventSubmitter build() {
      return new EventSubmitter(this);
    }
  }

  /**
   * GobblinEventBuilder namespace trumps over the namespace of the EventSubmitter unless it's null
   *
   * @param eventBuilder
   */
  public void submit(GobblinEventBuilder eventBuilder) {
    eventBuilder.addAdditionalMetadata(this.metadata);
    if(eventBuilder.namespace == null) {
      eventBuilder.setNamespace(this.namespace);
    }
    if (metricContext.isPresent()) {
      this.metricContext.get().submitEvent(eventBuilder.build());
    }
  }

  /**
   * This is a convenient way to submit an Event without using an EventSubmitter
   * namespace should never be null and is defaulted to {@link GobblinEventBuilder#NAMESPACE}
   * @param context
   * @param builder
   */
  public static void submit(MetricContext context, GobblinEventBuilder builder) {
    if(builder.namespace == null) {
      builder.setNamespace(GobblinEventBuilder.NAMESPACE);
    }
    context.submitEvent(builder.build());
  }

  private EventSubmitter(Builder builder) {
    this.metadata = builder.metadata;
    this.namespace = builder.namespace;
    this.metricContext = builder.metricContext;
  }


  /**
   * Submits the {@link org.apache.gobblin.metrics.GobblinTrackingEvent} to the {@link org.apache.gobblin.metrics.MetricContext}.
   * @param name Name of the event.
   * @deprecated Use {{@link #submit(GobblinEventBuilder)}}
   */
  @Deprecated
  public void submit(String name) {
    submit(name, ImmutableMap.<String, String>of());
  }


  /**
   * Calls submit on submitter if present.timing
   * @deprecated Use {{@link #submit(GobblinEventBuilder)}}
   */
  @Deprecated
  public static void submit(Optional<EventSubmitter> submitter, String name) {
    if (submitter.isPresent()) {
      submitter.get().submit(name);
    }
  }

  /**
   * Submits the {@link org.apache.gobblin.metrics.GobblinTrackingEvent} to the {@link org.apache.gobblin.metrics.MetricContext}.
   * @param name Name of the event.
   * @param metadataEls List of keys and values for metadata of the form key1, value2, key2, value2, ...
   * @deprecated Use {{@link #submit(GobblinEventBuilder)}}
   */
  @Deprecated
  public void submit(String name, String... metadataEls) {
    if(metadataEls.length % 2 != 0) {
      throw new IllegalArgumentException("Unmatched keys in metadata elements.");
    }

    Map<String, String> metadata = Maps.newHashMap();
    for(int i = 0; i < metadataEls.length/2; i++) {
      metadata.put(metadataEls[2 * i], metadataEls[2 * i + 1]);
    }
    submit(name, metadata);
  }

  /**
   * Calls submit on submitter if present.
   * @deprecated Use {{@link #submit(GobblinEventBuilder)}}
   */
  @Deprecated
  public static void submit(Optional<EventSubmitter> submitter, String name, String... metadataEls) {
    if (submitter.isPresent()) {
      submitter.get().submit(name, metadataEls);
    }
  }

  /**
   * Submits the {@link org.apache.gobblin.metrics.GobblinTrackingEvent} to the {@link org.apache.gobblin.metrics.MetricContext}.
   * @param name Name of the event.
   * @param additionalMetadata Additional metadata to be added to the event.
   * @deprecated Use {{@link #submit(GobblinEventBuilder)}}
   */
  @Deprecated
  public void submit(String name, Map<String, String> additionalMetadata) {
    if(this.metricContext.isPresent()) {
      Map<String, String> finalMetadata = Maps.newHashMap(this.metadata);
      if(!additionalMetadata.isEmpty()) {
        finalMetadata.putAll(additionalMetadata);
      }

      // Timestamp is set by metric context.
      this.metricContext.get().submitEvent(new GobblinTrackingEvent(0L, this.namespace, name, finalMetadata));
    }
  }

  /**
   * Calls submit on submitter if present.
   * @deprecated Use {{@link #submit(GobblinEventBuilder)}}
   */
  @Deprecated
  public static void submit(Optional<EventSubmitter> submitter, String name, Map<String, String> additionalMetadata) {
    if (submitter.isPresent()) {
      submitter.get().submit(name, additionalMetadata);
    }
  }

  /**
   * Get a {@link org.apache.gobblin.metrics.event.TimingEvent} attached to this {@link org.apache.gobblin.metrics.event.EventSubmitter}.
   * @param name Name of the {@link org.apache.gobblin.metrics.GobblinTrackingEvent} that will be generated.
   * @return a {@link org.apache.gobblin.metrics.event.TimingEvent}.
   * @deprecated Use {{@link TimingEvent)}}
   */
  @Deprecated
  public TimingEvent getTimingEvent(String name) {
    return new TimingEvent(this, name);
  }

  public List<Tag<?>> getTags() {
    return this.metricContext.isPresent() ?
        this.metricContext.get().getTags() :
        null;
  }

  /**
   * Serializer for jackson that does its best to serialize a {@link EventSubmitter} from a json representation.
   * NOTE: Due to the nested bidirectional nature of the {@link org.apache.gobblin.metrics.InnerMetricContext} and
   * usage of {@link Gauge} this is not a perfect serialization but it is sufficient for passing important monitoring
   * like original Azkaban flow name and gobblin job id
   */
  public static class Serializer extends StdSerializer<EventSubmitter> {
    public static final String CLASS_KEY = "class";
    public Serializer() {
      this(null);
    }

    public Serializer(Class<EventSubmitter> t) {
      super(t);
    }

    @Override
    public void serialize(EventSubmitter value, JsonGenerator jgen, SerializerProvider provider)
        throws IOException {
      jgen.writeStartObject();
      jgen.writeStringField("callerClass", value.getTags().stream().filter(tag -> tag.getKey().equals(
          CLASS_KEY)).findAny().map(tag -> String.valueOf(tag.getValue())).orElse(getClass().getName()));
      jgen.writeStringField("namespace", value.getNamespace());
      jgen.writeObjectField("tags", value.getTags());
      jgen.writeEndObject();
    }
  }

  public static class Deserializer extends StdDeserializer<EventSubmitter> {
    public Deserializer() {
      this(null);
    }

    public Deserializer(Class<EventSubmitter> t) {
      super(t);
    }

    @Override
    public EventSubmitter deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException {
      JsonNode node = jp.getCodec().readTree(jp);

      String namespace = node.get("namespace").asText();
      ObjectMapper objectMapper = new ObjectMapper();
      JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, Tag.class);
      List<Tag<?>> tags = objectMapper.readValue(node.get("tags").toString(), type);

      String callerClassStr = node.get("callerClass").asText();
      Class callerClass;
      try {
        callerClass = Class.forName(callerClassStr);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      // NOTE: This metric context is constructed differently from some other uses cases AbstractJobLauncher,
      // which uses JobMetrics.get() instead. Or the GobblinOrcWriter, which uses Instrumented.getMetricContext
      // The reason for is that we cannot create a circular dependency on the metrics module when adding the deserializer
      int randomId = new Random().nextInt(Integer.MAX_VALUE);
      MetricContext metricContext = MetricContext.builder(callerClass.getCanonicalName() + "." + randomId).addTags(tags).build();
      return new Builder(metricContext, namespace).build();
    }
  }
}
