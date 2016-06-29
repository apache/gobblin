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

package gobblin.kafka;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.avro.Schema;

import gobblin.kafka.schemareg.KafkaSchemaRegistry;
import gobblin.kafka.schemareg.SchemaRegistryException;
import gobblin.kafka.serialize.MD5Digest;


/**
 * A Mock SchemaRegistry that hands out MD5 based ids
 */
public class MockMd5SchemaRegistry implements KafkaSchemaRegistry<MD5Digest> {

  private final HashMap<MD5Digest, Schema> _schemaHashMap = new HashMap<>();
  private final HashMap<String, Schema> _nameSchemaHashMap = new HashMap<>();

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

  public MockMd5SchemaRegistry(String name, Schema schema)
      throws IOException, SchemaRegistryException {
    this.register(name, schema);
  }

  public MockMd5SchemaRegistry(Properties props)
  {
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
  public MD5Digest register(String name, Schema schema)
      throws IOException, SchemaRegistryException {

    MD5Digest md5Digest = generateId(schema);
    if (!_schemaHashMap.containsKey(md5Digest)) {
      _schemaHashMap.put(md5Digest, schema);
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
   * Get the latest schema that was registered under this name
   * @param name
   * @return
   * @throws SchemaRegistryException
   */
  @Override
  public Schema getLatestSchema(String name)
      throws IOException, SchemaRegistryException {
    return null;
  }

  /**
   *
   * SchemaRegistry implementations that do not have an internal cache can set this to false
   * and Gobblin will supplement such registries with a cache on top (if enabled).
   * @return whether this implementation of the schema registry has an internal cache
   */
  @Override
  public boolean hasInternalCache() {
    return false;
  }
}
