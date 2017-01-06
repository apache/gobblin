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

package gobblin.kafka.schemareg;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.avro.Schema;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.kafka.serialize.MD5Digest;


/**
 * A SchemaRegistry that hands out MD5 based ids based on configuration
 * Can be configured to be initialized with a single schema name, value pair.
 * {@see ConfigDrivenMd5SchemaRegistry.ConfigurationKeys} for configuration.
 *
 */
public class ConfigDrivenMd5SchemaRegistry implements KafkaSchemaRegistry<MD5Digest, Schema> {

  private static class ConfigurationKeys {
    private static final String SCHEMA_NAME_KEY="schemaRegistry.schema.name";
    private static final String SCHEMA_VALUE_KEY="schemaRegistry.schema.value";
  }

  private final HashMap<MD5Digest, Schema> _schemaHashMap = new HashMap<>();
  private final HashMap<String, Schema> _topicSchemaMap = new HashMap<>();

  private final MD5Digest generateId(Schema schema) {
    try {
      byte[] schemaBytes = schema.toString().getBytes("UTF-8");
      byte[] md5bytes = MessageDigest.getInstance("MD5").digest(schemaBytes);
      MD5Digest md5Digest = MD5Digest.fromBytes(md5bytes);
      return md5Digest;
    } catch (UnsupportedEncodingException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Unexpected error trying to convert schema to bytes", e);
    }
  }

  public ConfigDrivenMd5SchemaRegistry(String name, Schema schema)
      throws IOException, SchemaRegistryException {
    this.register(name, schema);
  }

  public ConfigDrivenMd5SchemaRegistry(Properties props)
      throws IOException, SchemaRegistryException {
    this(ConfigFactory.parseProperties(props));
  }

  public ConfigDrivenMd5SchemaRegistry(Config config)
      throws IOException, SchemaRegistryException {
    if (config.hasPath(ConfigurationKeys.SCHEMA_NAME_KEY)) {
      String name = config.getString(ConfigurationKeys.SCHEMA_NAME_KEY);
      String value = config.getString(ConfigurationKeys.SCHEMA_VALUE_KEY);
      Schema schema = new Schema.Parser().parse(value);
      register(name, schema);
    }
  }



  /**
   * Register this schema under the provided name
   * @param name
   * @param schema
   * @return
   * @throws IOException
   * @throws SchemaRegistryException
   */
  @Override
  public synchronized MD5Digest register(String name, Schema schema)
      throws IOException, SchemaRegistryException {

    MD5Digest md5Digest = generateId(schema);
    if (!_schemaHashMap.containsKey(md5Digest)) {
      _schemaHashMap.put(md5Digest, schema);
      _topicSchemaMap.put(name, schema);
    }
    return md5Digest;
  }

  /**
   * Get a schema given an id
   * @param id
   * @return
   * @throws IOException
   * @throws SchemaRegistryException
   */
  @Override
  public Schema getById(MD5Digest id)
      throws IOException, SchemaRegistryException {
      if (_schemaHashMap.containsKey(id))
      {
        return _schemaHashMap.get(id);
      }
      else
      {
        throw new SchemaRegistryException("Could not find schema with id : " + id.asString());
      }
    }

  /**
   * Will throw a SchemaRegistryException if it cannot find any schema for the provided name.
   * {@inheritDoc}
   */
  @Override
  public Schema getLatestSchema(String name)
      throws IOException, SchemaRegistryException {
    if (_topicSchemaMap.containsKey(name))
    {
      return _topicSchemaMap.get(name);
    }
    else
    {
      throw new SchemaRegistryException("Could not find any schema for " + name);
    }
  }

  /**
   *
   * {@inheritDoc}
   * @return false
   */
  @Override
  public boolean hasInternalCache() {
    return true;
  }
}
