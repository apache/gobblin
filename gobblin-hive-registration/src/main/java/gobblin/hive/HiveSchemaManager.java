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

package gobblin.hive;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

import gobblin.configuration.State;
import gobblin.hive.avro.HiveAvroSchemaManager;
import lombok.extern.slf4j.Slf4j;


/**
 * This class manages the schema for Hive registration.
 *
 * @author ziliu
 */
@Slf4j
public abstract class HiveSchemaManager {

  protected final State props;

  protected HiveSchemaManager(State props) {
    this.props = props;
  }

  /**
   * Add the appropriate schema properties to the given {@link SerDeInfo}.
   *
   * For example, when registering Avro tables, one needs to specify either
   * {@link org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties#SCHEMA_LITERAL}
   * or {@link org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils.AvroTableProperties#SCHEMA_URL}
   * in the {@link SerDeInfo}.
   */
  public abstract void addSchemaProperties(SerDeInfo si, Path path);

  public enum Implementation {
    NOP(HiveNopSchemaManager.class.getName()),
    AVRO(HiveAvroSchemaManager.class.getName());

    private final String schemaManagerClassName;

    private Implementation(String schemaManagerClassName) {
      this.schemaManagerClassName = schemaManagerClassName;
    }

    @Override
    public String toString() {
      return this.schemaManagerClassName;
    }
  }

  /**
   * Get an instance of {@link HiveSchemaManager}.
   *
   * @param type The {@link HiveSchemaManager} type. It could be AVRO, NOP or the name of a class that implements
   * {@link HiveSchemaManager}. The specified {@link HiveSchemaManager} type must have a constructor that takes a
   * {@link State} object.
   * @param props A {@link State} object used to instantiate {@link HiveSchemaManager}.
   */
  public static HiveSchemaManager getInstance(String type, State props) {
    Optional<Implementation> implementation = Enums.getIfPresent(Implementation.class, type.toUpperCase());
    if (implementation.isPresent()) {
      try {
        return (HiveSchemaManager) ConstructorUtils.invokeConstructor(Class.forName(implementation.get().toString()),
            props);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(
            "Unable to instantiate " + HiveSchemaManager.class.getSimpleName() + " with type " + type, e);
      }
    } else {
      log.info(String.format("%s with type %s does not exist. Using %s", HiveSchemaManager.class.getSimpleName(), type,
          HiveNopSchemaManager.class.getSimpleName()));
      return new HiveNopSchemaManager(props);
    }
  }

}
