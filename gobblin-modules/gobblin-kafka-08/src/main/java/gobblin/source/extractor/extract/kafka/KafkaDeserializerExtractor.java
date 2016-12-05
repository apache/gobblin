/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;

import kafka.message.MessageAndOffset;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import gobblin.annotation.Alias;
import gobblin.configuration.WorkUnitState;
import gobblin.metrics.kafka.KafkaSchemaRegistry;
import gobblin.metrics.kafka.SchemaRegistryException;
import gobblin.util.PropertiesUtils;


/**
 * <p>
 *   Extension of {@link KafkaExtractor} that wraps Kafka's {@link Deserializer} API. Kafka's {@link Deserializer} provides
 *   a generic way of converting Kafka {@link kafka.message.Message}s to {@link Object}. Typically, a {@link Deserializer}
 *   will be used along with a {@link org.apache.kafka.common.serialization.Serializer} which is responsible for converting
 *   an {@link Object} to a Kafka {@link kafka.message.Message}. These APIs are useful for reading and writing to Kafka,
 *   since Kafka is primarily a byte oriented system.
 * </p>
 *
 * <p>
 *   This class wraps the {@link Deserializer} API allowing any existing classes that implement the {@link Deserializer}
 *   API to integrate with seamlessly with Gobblin. The deserializer can be specified in the following ways:
 *
 *   <ul>
 *     <li>{@link #KAFKA_DESERIALIZER_TYPE} can be used to specify a pre-defined enum from {@link Deserializers} or
 *     it can be used to specify the fully-qualified name of a {@link Class} that defines the {@link Deserializer}
 *     interface. If this property is set to a class name, then {@link KafkaSchemaRegistry} must also be specified
 *     using the {@link KafkaSchemaRegistry#KAFKA_SCHEMA_REGISTRY_CLASS} config key</li>
 *   </ul>
 * </p>
 */
@Getter(AccessLevel.PACKAGE)
@Alias(value = "DESERIALIZER")
public class KafkaDeserializerExtractor extends KafkaExtractor<Object, Object> {

  public static final String KAFKA_DESERIALIZER_TYPE = "kafka.deserializer.type";

  private static final String CONFLUENT_SCHEMA_REGISTRY_URL = "schema.registry.url";

  private final Deserializer<?> kafkaDeserializer;
  private final KafkaSchemaRegistry<?, ?> kafkaSchemaRegistry;

  public KafkaDeserializerExtractor(WorkUnitState state) throws ReflectiveOperationException {
    this(state, getDeserializer(getProps(state)),
        getKafkaSchemaRegistry(getProps(state)));
  }

  @VisibleForTesting
  KafkaDeserializerExtractor(WorkUnitState state, Deserializer<?> kafkaDeserializer,
      KafkaSchemaRegistry<?, ?> kafkaSchemaRegistry) {
    super(state);
    this.kafkaDeserializer = kafkaDeserializer;
    this.kafkaSchemaRegistry = kafkaSchemaRegistry;
  }

  @Override
  protected Object decodeRecord(MessageAndOffset messageAndOffset) throws IOException {
    return this.kafkaDeserializer.deserialize(this.topicName, getBytes(messageAndOffset.message().payload()));
  }

  @Override
  public Object getSchema() throws IOException {
    try {
      return this.kafkaSchemaRegistry.getLatestSchemaByTopic(this.topicName);
    } catch (SchemaRegistryException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Constructs a {@link Deserializer}, using the value of {@link #KAFKA_DESERIALIZER_TYPE}.
   */
  private static Deserializer<?> getDeserializer(Properties props) throws ReflectiveOperationException {

    Preconditions.checkArgument(props.containsKey(KAFKA_DESERIALIZER_TYPE),
        "Missing required property " + KAFKA_DESERIALIZER_TYPE);
    Optional<Deserializers> deserializerType =
        Enums.getIfPresent(Deserializers.class, props.getProperty(KAFKA_DESERIALIZER_TYPE).toUpperCase());
    Deserializer<?> deserializer;

    if (deserializerType.isPresent()) {
      deserializer = ConstructorUtils.invokeConstructor(deserializerType.get().getDeserializerClass());
    } else {
      deserializer = Deserializer.class
          .cast(ConstructorUtils.invokeConstructor(Class.forName(props.getProperty(KAFKA_DESERIALIZER_TYPE))));
    }
    deserializer.configure(PropertiesUtils.propsToStringKeyMap(props), false);
    return deserializer;
  }

  /**
   * Constructs a {@link KafkaSchemaRegistry} using the value of {@link #KAFKA_DESERIALIZER_TYPE}, if not set it
   * defaults to {@link SimpleKafkaSchemaRegistry}.
   */
  private static KafkaSchemaRegistry<?, ?> getKafkaSchemaRegistry(Properties props)
      throws ReflectiveOperationException {

    Optional<Deserializers> deserializerType =
        Enums.getIfPresent(Deserializers.class, props.getProperty(KAFKA_DESERIALIZER_TYPE).toUpperCase());

    if (deserializerType.isPresent()) {
      return ConstructorUtils.invokeConstructor(deserializerType.get().getSchemaRegistryClass(), props);
    }
    if (props.containsKey(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_CLASS)) {
      return KafkaSchemaRegistry.get(props);
    }
    return new SimpleKafkaSchemaRegistry(props);
  }

  /**
   * Gets {@link Properties} from a {@link WorkUnitState} and sets the config <code>schema.registry.url</code> to value
   * of {@link KafkaSchemaRegistry#KAFKA_SCHEMA_REGISTRY_URL} if set. This way users don't need to specify both
   * properties as <code>schema.registry.url</code> is required by the {@link ConfluentKafkaSchemaRegistry}.
   */
  private static Properties getProps(WorkUnitState workUnitState) {
    Properties properties = workUnitState.getProperties();
    if (properties.containsKey(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL)) {
      properties.setProperty(CONFLUENT_SCHEMA_REGISTRY_URL,
          properties.getProperty(KafkaSchemaRegistry.KAFKA_SCHEMA_REGISTRY_URL));
    }
    return properties;
  }

  /**
   * Pre-defined {@link Deserializer} that can be referenced by the enum name.
   */
  @AllArgsConstructor
  @Getter
  public enum Deserializers {

    /**
     * Confluent's Avro {@link Deserializer}
     *
     * @see KafkaAvroDeserializer
     */
    CONFLUENT_AVRO(KafkaAvroDeserializer.class, ConfluentKafkaSchemaRegistry.class),

    /**
     * Confluent's JSON {@link Deserializer}
     *
     * @see KafkaJsonDeserializer
     */
    CONFLUENT_JSON(KafkaJsonDeserializer.class, SimpleKafkaSchemaRegistry.class),

    /**
     * A custom {@link Deserializer} for converting <code>byte[]</code> to {@link com.google.gson.JsonElement}s
     *
     * @see KafkaGsonDeserializer
     */
    GSON(KafkaGsonDeserializer.class, SimpleKafkaSchemaRegistry.class),

    /**
     * A standard Kafka {@link Deserializer} that does nothing, it simply returns the <code>byte[]</code>
     */
    BYTE_ARRAY(ByteArrayDeserializer.class, SimpleKafkaSchemaRegistry.class),

    /**
     * A standard Kafka {@link Deserializer} for converting <code>byte[]</code> to {@link String}s
     */
    STRING(StringDeserializer.class, SimpleKafkaSchemaRegistry.class);

    private final Class<? extends Deserializer> deserializerClass;
    private final Class<? extends KafkaSchemaRegistry> schemaRegistryClass;
  }
}
