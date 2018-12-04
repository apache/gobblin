package org.apache.gobblin.metrics.kafka;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import org.apache.commons.lang3.tuple.Pair;
import com.google.common.base.Splitter;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import org.apache.gobblin.metrics.GobblinTrackingEvent;
import org.apache.gobblin.metrics.MetricContext;


@Slf4j
public abstract class KafkaEventKeyValueReporter extends KafkaEventReporter {
  private static final Splitter COMMA_SEPARATOR = Splitter.on(",").omitEmptyStrings().trimResults();
  private Optional<List<String>> keyName = Optional.absent();
  public static final String keyPropertyName = "metrics.reporting.kafkaPusherKeys";

  public KafkaEventKeyValueReporter(BuilderImpl builder) throws IOException {
    super(builder);
    if (builder.keyName.size() > 0) {
      this.keyName = Optional.of(builder.keyName);
    }
  }

  public static abstract class BuilderImpl extends KafkaEventReporter.BuilderImpl {
    protected List<String> keyName = Lists.newArrayList();

    public BuilderImpl(MetricContext context, Properties properties) {
      super(context);
      if (properties.containsKey(keyPropertyName)) {
        this.keyName = COMMA_SEPARATOR.splitToList(properties.getProperty(keyPropertyName));
      } else {
        log.warn("Keys for KafkaEventKeyValueReporter are not provided. Without keys, it will act like a KafkaAvroEventReporter.");
      }
    }

    @Override
    public abstract KafkaEventReporter build(String brokers, String topic) throws IOException;

    @Override
    protected KafkaEventKeyValueReporter.BuilderImpl self() {
      return this;
    }
  }


  @Override
  public void reportEventQueue(Queue<GobblinTrackingEvent> queue) {
    GobblinTrackingEvent nextEvent;
    List<Pair<String, byte[]>> events = com.google.common.collect.Lists.newArrayList();

    while(null != (nextEvent = queue.poll())) {
      StringBuilder sb = new StringBuilder();
      String key = null;
      if (keyName.isPresent()) {
        try {
          for (String keyPart : keyName.get()) {
            if (nextEvent.getMetadata().containsKey(keyPart)) {
              sb.append(nextEvent.getMetadata().get(keyPart));
            } else {
              throw new KafkaEventKeyValueReporter.KeyPartNotFoundException(keyPart);
            }
          }
          key = sb.toString();
        } catch (KafkaEventKeyValueReporter.KeyPartNotFoundException e) {
          log.error("{} not found in the GobblinTrackingEvent. Setting key to null.", e.expectedKey);
          key = null;
        }
      }
      events.add(Pair.of(key, this.serializer.serializeRecord(nextEvent)));
    }

    if (!events.isEmpty()) {
      this.kafkaPusher.pushMessages(events);
    }
  }

  private static class KeyPartNotFoundException extends IllegalArgumentException {
    private final String expectedKey;
    KeyPartNotFoundException(String key) {
      this.expectedKey = key;
    }
  }
}
