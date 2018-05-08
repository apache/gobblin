package org.apache.gobblin.event;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.gobblin.metrics.kafka.LoggingPusher;
import org.apache.gobblin.metrics.kafka.Pusher;
import org.apache.gobblin.util.ConfigUtils;


/**
 * A event producer generates an event and pushes it out. Its pusher is specified by configuration
 * {@link #PUSHER_CLASS}, whose default value is a {@link LoggingPusher}
 */
public class EventProducer<T> implements Closeable {
  public static final String PUSHER_CONFIG = "pusherConfig";

  private static final String PUSHER_CLASS = "pusherClass";
  private static final Config FALLBACK = ConfigFactory.parseMap(
      ImmutableMap.<String, Object>builder()
          .put(PUSHER_CLASS, LoggingPusher.class.getName())
          .build());

  protected final Closer closer;

  private final Pusher<T> pusher;

  public EventProducer(Config cfg) {
    Config config = cfg.withFallback(FALLBACK);

    String pusherClass = config.getString(PUSHER_CLASS);
    Config pusherConfig = ConfigUtils.getConfigOrEmpty(config, PUSHER_CONFIG).withFallback(config);
    closer = Closer.create();
    try {
      pusher = closer.register((Pusher) ConstructorUtils.invokeConstructor(Class.forName(pusherClass), pusherConfig));
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException("Could not instantiate pusher " + pusherClass, e);
    }
  }

  public void produceEvent(T event) {
    pusher.pushMessages(ImmutableList.of(event));
  }

  @Override
  public void close()
      throws IOException {
    this.closer.close();
  }
}
