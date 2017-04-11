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

package gobblin.kafka.writer;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.commons.lang3.reflect.ConstructorUtils;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import lombok.extern.slf4j.Slf4j;

import gobblin.util.ConfigUtils;

import static gobblin.kafka.writer.KafkaWriterConfigurationKeys.*;
import static gobblin.kafka.writer.KafkaWriterConfigurationKeys.CLIENT_ID_DEFAULT;


/**
 * Helper class for version-specific Kafka writers
 */
@Slf4j
public class KafkaWriterHelper {

  static Properties getProducerProperties(Properties props)
  {
    Properties producerProperties = stripPrefix(props, KAFKA_PRODUCER_CONFIG_PREFIX);

    // Provide default properties if not set from above
    setDefaultIfUnset(producerProperties, KEY_SERIALIZER_CONFIG, DEFAULT_KEY_SERIALIZER);
    setDefaultIfUnset(producerProperties, VALUE_SERIALIZER_CONFIG, DEFAULT_VALUE_SERIALIZER);
    setDefaultIfUnset(producerProperties, CLIENT_ID_CONFIG, CLIENT_ID_DEFAULT);
    return producerProperties;
  }

  private static void setDefaultIfUnset(Properties props, String key, String value)
  {
    if (!props.containsKey(key)) {
      props.setProperty(key, value);
    }
  }

  private static Properties stripPrefix(Properties props, String prefix) {
    Properties strippedProps = new Properties();
    int prefixLength = prefix.length();
    for (String key: props.stringPropertyNames())
    {
      if (key.startsWith(prefix))
      {
        strippedProps.setProperty(key.substring(prefixLength), props.getProperty(key));
      }
    }
    return strippedProps;
  }

  public static Object getKafkaProducer(Properties props)
  {
    Config config = ConfigFactory.parseProperties(props);
    String kafkaProducerClass = ConfigUtils
        .getString(config, KafkaWriterConfigurationKeys.KAFKA_WRITER_PRODUCER_CLASS,
            KafkaWriterConfigurationKeys.KAFKA_WRITER_PRODUCER_CLASS_DEFAULT);
    Properties producerProps = getProducerProperties(props);
    try {
      Class<?> producerClass = (Class<?>) Class.forName(kafkaProducerClass);
      Object producer = ConstructorUtils.invokeConstructor(producerClass, producerProps);
      return producer;
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
      log.error("Failed to instantiate Kafka producer from class " + kafkaProducerClass, e);
      throw Throwables.propagate(e);
    }
  }


}
